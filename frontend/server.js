import express from 'express'
import { readdir, readFile } from 'fs/promises'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'
import { existsSync } from 'fs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PIPELINE_DATA = resolve(__dirname, '..', 'pipeline', 'data')
const OUTPUT_DIR = join(PIPELINE_DATA, 'output')
const STAGING_DIR = join(PIPELINE_DATA, 'staging')

const app = express()
const PORT = process.env.PORT || 3001

// --- Helpers ---

async function readJsonSafe(filePath) {
  try {
    const raw = await readFile(filePath, 'utf-8')
    return JSON.parse(raw)
  } catch {
    return null
  }
}

function deriveStagingDir(outputDirName) {
  // Output dir: {stem}_{YYYYMMDD_HHMMSS}
  // Staging dir: {stem} (without the timestamp suffix)
  const match = outputDirName.match(/^(.+)_\d{8}_\d{6}$/)
  return match ? match[1] : outputDirName
}

function resolvePlaceholders(text, mapping) {
  if (!text || !mapping) return text
  let resolved = String(text)
  for (const [placeholder, original] of Object.entries(mapping)) {
    resolved = resolved.replaceAll(placeholder, original)
  }
  return resolved
}

function cleanNullString(val) {
  return val === 'null' ? null : val
}

// --- API Routes ---

// List all reports
app.get('/api/reports', async (_req, res) => {
  try {
    const entries = await readdir(OUTPUT_DIR, { withFileTypes: true })
    const dirs = entries.filter(e => e.isDirectory()).map(e => e.name).sort()

    const reports = []
    for (const dir of dirs) {
      const results = await readJsonSafe(join(OUTPUT_DIR, dir, 'pipeline_results.json'))
      if (!results) continue

      const k1 = results.k1_data || {}
      const analysis = results.financial_analysis || {}
      const pii = results.pii_stats || {}
      const meta = results.processing_metadata || {}

      // Load placeholder mapping from staging to resolve names
      const runId = deriveStagingDir(dir)
      const sanitized = await readJsonSafe(join(STAGING_DIR, runId, 'sanitized_text.json'))
      const mapping = sanitized?.placeholder_mapping || null

      reports.push({
        directory: dir,
        partnership_name: resolvePlaceholders(k1.partnership_name, mapping) || dir,
        partner_type: cleanNullString(k1.partner_type) || null,
        tax_year: k1.tax_year || null,
        net_taxable_income: analysis.net_taxable_income ?? null,
        total_income: analysis.total_income ?? null,
        capital_account_ending: k1.capital_account_ending ?? null,
        pii_entities: pii.total_entities_detected || 0,
        processed_at: meta.report_generated_at || null,
      })
    }

    res.json({ total: reports.length, reports })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Get full report detail
app.get('/api/reports/:dirName', async (req, res) => {
  const { dirName } = req.params
  const outputPath = join(OUTPUT_DIR, dirName)

  if (!existsSync(outputPath)) {
    return res.status(404).json({ error: 'Report not found' })
  }

  // Load output files
  const pipelineResults = await readJsonSafe(join(outputPath, 'pipeline_results.json'))
  const k1Report = await readJsonSafe(join(outputPath, 'k1_report.json'))

  if (!pipelineResults) {
    return res.status(404).json({ error: 'pipeline_results.json not found' })
  }

  // Load staging files
  const runId = deriveStagingDir(dirName)
  const stagingPath = join(STAGING_DIR, runId)

  const sanitizedText = await readJsonSafe(join(stagingPath, 'sanitized_text.json'))
  const ocrText = await readJsonSafe(join(stagingPath, 'ocr_text.json'))
  const piiReport = await readJsonSafe(join(stagingPath, 'pii_report.json'))

  res.json({
    directory: dirName,
    pipeline_results: pipelineResults,
    ai_interactions: k1Report?.ai_interactions || null,
    placeholder_mapping: sanitizedText?.placeholder_mapping || null,
    sanitized_text: sanitizedText?.sanitized_text || null,
    ocr_text: ocrText?.full_text || null,
    ocr_pages: ocrText?.pages || null,
    pii_report_detail: piiReport || null,
  })
})

// --- Static files (production) ---

const distPath = join(__dirname, 'dist')
if (existsSync(distPath)) {
  app.use(express.static(distPath))
  app.get('/{*path}', (_req, res) => {
    res.sendFile(join(distPath, 'index.html'))
  })
}

app.listen(PORT, () => {
  console.log(`K-1 Audit Server running at http://localhost:${PORT}`)
  console.log(`Reading pipeline data from: ${PIPELINE_DATA}`)
})
