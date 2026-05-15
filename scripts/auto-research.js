#!/usr/bin/env node
/**
 * auto-research.js
 * Universal Data Scraper for Serverless Research Project
 * 
 * Usage:
 *   Local:   node scripts/auto-research.js
 *   Cloudflare: Wrangler will inject 'env' with DB
 *   AWS/Oracle: DB connection string passed via ENV vars
 * 
 * Architecture:
 *   Uses a DatabaseAdapter pattern to support SQLite (D1), MySQL, and Postgres.
 */

import fs from 'fs';
import path from 'path';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- CONFIGURATION ---
const CONFIG_FILE = path.join(__dirname, '..', 'data', 'urls.json');
const DELAY_MS = 1500; // Be polite to target servers

// --- DATABASE ADAPTER INTERFACE ---
// Implementations for D1, MySQL, and Postgres would go here.
// For this script, we assume the 'db' object is injected by the environment.

class DatabaseAdapter {
  constructor(db) {
    this.db = db;
  }

  async init() {
    // Ensure tables exist (idempotent)
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS institutions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        home_url TEXT,
        biz_url TEXT,
        cc_url TEXT,
        logo_icon TEXT,
        color_hex TEXT,
        insurance_type TEXT,
        last_updated TEXT,
        biz_chk_apy REAL,
        biz_chk_min_open REAL,
        biz_chk_min_keep REAL,
        biz_chk_min_earn REAL,
        biz_chk_fee_mo REAL,
        biz_chk_tx_limit INTEGER,
        biz_chk_tx_overage REAL,
        biz_chk_cash_dep INTEGER,
        biz_chk_atm_net TEXT,
        biz_chk_zelle INTEGER,
        biz_sav_apy REAL,
        biz_mm_apy REAL,
        biz_mm_min_earn REAL,
        wire_in_fee REAL,
        wire_out_fee REAL,
        od_protection TEXT,
        merch_platform TEXT,
        merch_setup TEXT,
        merch_hw_cost TEXT,
        merch_fees TEXT,
        merch_settle TEXT,
        cc_name TEXT,
        cc_apr_min REAL,
        cc_apr_max REAL,
        cc_ann_fee REAL,
        cc_benefit TEXT,
        is_estimated INTEGER,
        notes TEXT
      );

      CREATE TABLE IF NOT EXISTS rate_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        institution_id TEXT,
        metric_name TEXT,
        old_value TEXT,
        new_value TEXT,
        changed_at TEXT,
        source TEXT,
        FOREIGN KEY(institution_id) REFERENCES institutions(id)
      );
    `);
  }

  async getInstitution(id) {
    const stmt = this.db.prepare('SELECT * FROM institutions WHERE id = ?');
    return await stmt.get(id);
  }

  async updateInstitution(data) {
    const fields = Object.keys(data).filter(k => k !== 'id');
    const placeholders = fields.map(() => '?').join(', ');
    const values = fields.map(k => data[k]);
    values.push(data.id);

    const sql = `UPDATE institutions SET ${fields.map(f => `${f} = ?`).join(', ')} WHERE id = ?`;
    await this.db.prepare(sql).run(...values);
  }

  async insertHistory(institutionId, metric, oldValue, newValue) {
    const sql = `INSERT INTO rate_history (institution_id, metric_name, old_value, new_value, changed_at, source) VALUES (?, ?, ?, ?, datetime('now'), 'auto-scraper')`;
    await this.db.prepare(sql).run(institutionId, metric, String(oldValue), String(newValue));
  }

  async upsertInstitution(data) {
    const existing = await this.getInstitution(data.id);
    
    if (existing) {
      // Check for changes before updating
      const changes = [];
      Object.keys(data).forEach(key => {
        if (key === 'id' || key === 'last_updated') return;
        const oldVal = existing[key];
        const newVal = data[key];
        
        // Handle null/undefined comparison
        if (String(oldVal) !== String(newVal)) {
          changes.push({ key, oldVal, newVal });
        }
      });

      if (changes.length > 0) {
        // Log history
        for (const change of changes) {
          await this.insertHistory(data.id, change.key, change.oldVal, change.newVal);
        }
        
        // Update record
        await this.updateInstitution(data);
        return { action: 'updated', changes: changes.length };
      }
      return { action: 'unchanged' };
    } else {
      // Insert new
      const fields = Object.keys(data).join(', ');
      const placeholders = Object.keys(data).map(() => '?').join(', ');
      const values = Object.values(data);
      
      const sql = `INSERT INTO institutions (${fields}) VALUES (${placeholders})`;
      await this.db.prepare(sql).run(...values);
      return { action: 'inserted' };
    }
  }
}

// --- PARSERS ---
// Placeholder parsers - replace selectors with real ones from target sites
function parseTarget($, url, id) {
  const result = { id, name: '', last_updated: new Date().toISOString().split('T')[0] };
  
  // Example: Generic fallback
  const text = $('body').text();
  
  // Heuristic: Look for APY patterns
  const apyMatch = text.match(/(\d+\.\d+)\s*%.*APY/i);
  if (apyMatch) result.biz_chk_apy = parseFloat(apyMatch[1]);

  // Heuristic: Look for MM rates
  const mmMatch = text.match(/(\d+\.\d+)\s*%.*Money Market/i);
  if (mmMatch) result.biz_mm_apy = parseFloat(mmMatch[1]);

  // Heuristic: Look for CC APR
  const aprMatch = text.match(/(\d+\.\d+)%.*APR/i);
  if (aprMatch) result.cc_apr_min = parseFloat(aprMatch[1]);

  return result;
}

// --- MAIN LOGIC ---

async function run(dbAdapter) {
  console.log(`[${new Date().toISOString()}] Starting research scrape...`);
  
  // Load URLs
  let urlsData;
  try {
    const content = fs.readFileSync(CONFIG_FILE, 'utf-8');
    urlsData = JSON.parse(content);
  } catch (e) {
    console.error("Failed to load urls.json", e);
    process.exit(1);
  }

  const processed = [];
  const errors = [];

  for (const [id, config] of Object.entries(urlsData)) {
    console.log(`Processing: ${config.name} (${id})`);
    
    if (!config.rates || config.rates.length === 0) {
      console.log(`  [SKIP] No rates URL.`);
      continue;
    }

    let html = '';
    let success = false;

    for (const url of config.rates) {
      try {
        const res = await axios.get(url, {
          headers: { 'User-Agent': 'Mozilla/5.0 (ResearchBot/1.0; +https://alittlealoha.pro)' },
          timeout: 10000
        });
        html = res.data;
        success = true;
        break;
      } catch (err) {
        console.warn(`  [WARN] Failed ${url}: ${err.message}`);
      }
    }

    if (!success) {
      errors.push({ id, reason: 'Fetch failed' });
      continue;
    }

    const $ = cheerio.load(html);
    const newData = parseTarget($, config.rates[0], id);
    
    // Merge with existing metadata if needed (e.g., logos, colors)
    // In a real scenario, you'd load the existing record first to preserve static fields
    // For now, we assume newData has enough or we merge with a cached static config
    
    const result = await dbAdapter.upsertInstitution(newData);
    console.log(`  [DONE] ${result.action} (${result.changes || 0} changes)`);
    processed.push(id);

    await new Promise(r => setTimeout(r, DELAY_MS));
  }

  console.log(`\n--- Summary ---`);
  console.log(`Processed: ${processed.length}`);
  console.log(`Errors: ${errors.length}`);
  if (errors.length > 0) {
    console.log(`Failed IDs: ${errors.map(e => e.id).join(', ')}`);
  }
}

// --- EXPORT FOR DIFFERENT ENVIRONMENTS ---

// 1. For Local/CLI usage (requires a mock DB or file-based DB)
if (process.env.NODE_ENV === 'local') {
  // Mock DB for local testing (using better-sqlite3 or similar)
  // This block would be implemented with a real SQLite driver for local dev
  console.log("Local mode not fully implemented in this snippet. Run on platform.");
}

// 2. For Cloudflare Workers (export default)
export default {
  async fetch(request, env, ctx) {
    const adapter = new DatabaseAdapter(env.DB); // env.DB is the D1 binding
    await adapter.init();
    await run(adapter);
    return new Response("Scrape complete");
  },
  async scheduled(event, env, ctx) {
    const adapter = new DatabaseAdapter(env.DB);
    await adapter.init();
    await run(adapter);
  }
};

// 3. For AWS Lambda / Oracle Node (entry point)
if (typeof module !== 'undefined' && require.main === module) {
  // This block runs when executed directly via 'node auto-research.js'
  // Requires a real DB connection string from ENV
  const { Client } = require('pg'); // Or mysql2
  const connectionString = process.env.DATABASE_URL;
  
  if (!connectionString) {
    console.error("DATABASE_URL not set. Exiting.");
    process.exit(1);
  }

  const client = new Client({ connectionString });
  await client.connect();
  
  // Wrap client in adapter
  const adapter = new DatabaseAdapter({
    exec: async (sql) => client.query(sql),
    prepare: (sql) => ({
      run: async (...args) => client.query(sql, args),
      get: async (val) => {
        const res = await client.query(sql, [val]);
        return res.rows[0];
      }
    })
  });

  await run(adapter);
  await client.end();
}