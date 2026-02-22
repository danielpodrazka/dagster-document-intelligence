import express from 'express'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'
import { existsSync } from 'fs'
import { S3Client, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3'

const __dirname = dirname(fileURLToPath(import.meta.url))

const s3 = new S3Client({
  endpoint: process.env.AWS_ENDPOINT_URL || 'http://localhost:4566',
  region: process.env.AWS_DEFAULT_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'test',
  },
  forcePathStyle: true,
})
const BUCKET = process.env.S3_BUCKET_NAME || 'dagster-document-intelligence-etl'

const app = express()
const PORT = process.env.PORT || 3001

// --- Helpers ---

async function readS3Json(key) {
  try {
    const resp = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }))
    const text = await resp.Body.transformToString()
    return JSON.parse(text)
  } catch {
    return null
  }
}

async function listS3Prefixes(prefix) {
  const resp = await s3.send(new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: prefix,
    Delimiter: '/',
  }))
  return (resp.CommonPrefixes || []).map(p => p.Prefix.replace(prefix, '').replace(/\/$/, ''))
}

function deriveStagingDir(outputDirName) {
  // Output dir: {stem}_{YYYYMMDD_HHMMSS}
  // Staging dir: {stem} (without the timestamp suffix)
  const match = outputDirName.match(/^(.+)_\d{8}_\d{6}$/)
  return match ? match[1] : outputDirName
}

async function readStagingJson(runId, filename) {
  // Try run_id-scoped staging first, then fall back to root staging
  return await readS3Json(`staging/${runId}/${filename}`)
    || await readS3Json(`staging/${filename}`)
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
    const dirs = await listS3Prefixes('output/')

    const reports = []
    for (const dir of dirs.sort()) {
      const results = await readS3Json(`output/${dir}/pipeline_results.json`)
      if (!results) continue

      const k1 = results.k1_data || {}
      const analysis = results.financial_analysis || {}
      const pii = results.pii_stats || {}
      const meta = results.processing_metadata || {}

      // Load placeholder mapping from staging to resolve names
      const runId = deriveStagingDir(dir)
      const sanitized = await readStagingJson(runId, 'sanitized_text.json')
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
        validation_status: results.validation?.overall_status || null,
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

  // Load output files
  const pipelineResults = await readS3Json(`output/${dirName}/pipeline_results.json`)
  const k1Report = await readS3Json(`output/${dirName}/k1_report.json`)

  if (!pipelineResults) {
    return res.status(404).json({ error: 'Report not found' })
  }

  // Load staging files
  const runId = deriveStagingDir(dirName)

  const sanitizedText = await readStagingJson(runId, 'sanitized_text.json')
  const ocrText = await readStagingJson(runId, 'ocr_text.json')
  const piiReport = await readStagingJson(runId, 'pii_report.json')

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
  console.log(`Reading pipeline data from S3: ${BUCKET}`)
})
