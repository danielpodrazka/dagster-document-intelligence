import { useState, useEffect, useRef, useCallback } from 'react'
import './App.css'

// --- Helpers ---

const EIN_RE = /^\d{2}-\d{7}$/

function filterCrossPartnerForReport(cpData, placeholderMapping) {
  if (!cpData || !cpData.results || !placeholderMapping) return cpData

  // Extract partnership EIN from placeholder mapping (first EIN value)
  let partnershipEin = null
  const entries = Object.entries(placeholderMapping)
    .sort(([a], [b]) => a.localeCompare(b))
  for (const [, original] of entries) {
    if (EIN_RE.test(original.trim())) {
      partnershipEin = original.trim()
      break
    }
  }

  if (!partnershipEin) return cpData

  // Filter by partnership EIN only — not tax year, because multi-year
  // continuity checks (category B) span across years
  const filteredResults = cpData.results.filter(r => r.partnership_ein === partnershipEin)
  const filteredPartnerships = (cpData.partnerships_validated || []).filter(p =>
    p.partnership_ein === partnershipEin
  )

  const passed = filteredResults.filter(r => r.passed).length
  const failed = filteredResults.length - passed
  const critical = filteredResults.filter(r => !r.passed && r.severity === 'critical').length
  const warnings = filteredResults.filter(r => !r.passed && r.severity === 'warning').length
  const advisory = filteredResults.filter(r => !r.passed && r.severity === 'advisory').length

  return {
    ...cpData,
    summary: {
      total_checks: filteredResults.length,
      passed,
      failed,
      critical,
      warnings,
      advisory,
    },
    results: filteredResults,
    partnerships_validated: filteredPartnerships,
    year_pairs_checked: filteredPartnerships.length > 1 ? cpData.year_pairs_checked : 0,
  }
}

const formatCurrency = (value) => {
  if (value == null) return '$0'
  const num = typeof value === 'string' ? parseFloat(value) : value
  const sign = num < 0 ? '-' : ''
  return `${sign}$${Math.abs(num).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`
}

function resolvePlaceholders(text, mapping) {
  if (!text || !mapping) return text
  let resolved = String(text)
  for (const [placeholder, original] of Object.entries(mapping)) {
    resolved = resolved.replaceAll(placeholder, original)
  }
  return resolved
}

function cleanDirName(dir) {
  // Strip timestamp suffix for display
  return dir.replace(/_\d{8}_\d{6}$/, '').replace(/_/g, ' ')
}

// ================================================================
//  Report List (landing page)
// ================================================================

function ReportList({ onSelect }) {
  const [reports, setReports] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/reports')
      .then(r => r.json())
      .then(data => { setReports(data.reports || []); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading"><div className="loading-spinner" /><p>Loading reports...</p></div>

  if (reports.length === 0) {
    return (
      <div className="error-state">
        <h2>No Reports Found</h2>
        <p>Run the Dagster pipeline first to process K-1 documents.</p>
        <code>cd pipeline && dg dev</code>
        <p style={{ fontSize: '0.78rem', marginTop: '0.5rem', color: 'var(--text-muted)' }}>
          Then materialize all assets or drop PDFs into data/dropoff/
        </p>
      </div>
    )
  }

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>Processed K-1 Reports</h2>
        <span className="tag financial">{reports.length} reports</span>
      </div>
      <div className="card-body">
        <table className="report-list-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Partnership</th>
              <th>Type</th>
              <th>Year</th>
              <th style={{ textAlign: 'right' }}>Net Income</th>
              <th style={{ textAlign: 'right' }}>Capital End</th>
              <th style={{ textAlign: 'center' }}>PII</th>
              <th>Processed</th>
            </tr>
          </thead>
          <tbody>
            {reports.map((r, i) => (
              <tr key={r.directory} className="report-row" onClick={() => onSelect(r.directory)}>
                <td className="report-num">{String(i + 1).padStart(2, '0')}</td>
                <td className="report-name">{cleanDirName(r.directory)}</td>
                <td>{r.partner_type || '-'}</td>
                <td>{r.tax_year || '-'}</td>
                <td className={`report-amount ${(r.net_taxable_income || 0) < 0 ? 'amount-negative' : 'amount-positive'}`}>
                  {r.net_taxable_income != null ? formatCurrency(r.net_taxable_income) : '-'}
                </td>
                <td className="report-amount">
                  {r.capital_account_ending != null ? formatCurrency(r.capital_account_ending) : '-'}
                </td>
                <td style={{ textAlign: 'center' }}>{r.pii_entities}</td>
                <td>{r.processed_at ? new Date(r.processed_at).toLocaleDateString() : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ================================================================
//  Cross-Partner Dashboard (landing page)
// ================================================================

function CrossPartnerDashboard() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/cross-partner')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  if (loading || !data || !data.summary) return null

  return <CrossPartnerSection crossPartner={data} />
}

// ================================================================
//  Tab Navigation
// ================================================================

const TABS = [
  { key: 'summary', label: 'Summary' },
  { key: 'validation', label: 'Validation' },
  { key: 'pii', label: 'PII & Redactions' },
  { key: 'ai', label: 'AI Audit' },
  { key: 'ocr', label: 'OCR Text' },
  { key: 'meta', label: 'Metadata' },
]

function TabNav({ active, onChange }) {
  return (
    <div className="tab-nav">
      {TABS.map(t => (
        <button key={t.key} className={`tab-btn ${active === t.key ? 'active' : ''}`} onClick={() => onChange(t.key)}>
          {t.label}
        </button>
      ))}
    </div>
  )
}

// ================================================================
//  Tab 1: Summary
// ================================================================

function StatsGrid({ data, mapping }) {
  const k1 = data.k1_data || {}
  const analysis = data.financial_analysis || {}
  const piiStats = data.pii_stats || {}

  const capGains = (k1.long_term_capital_gains ?? 0) + (k1.short_term_capital_gains ?? 0)

  return (
    <div className="stats-grid">
      <div className="stat-card">
        <div className="stat-label">Total Income</div>
        <div className={`stat-value ${(analysis.total_income || 0) >= 0 ? 'positive' : 'negative'}`}>
          {formatCurrency(analysis.total_income || k1.ordinary_business_income)}
        </div>
        <div className="stat-sub">Ordinary + Capital + Other</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Net Capital Gains</div>
        <div className={`stat-value ${capGains >= 0 ? 'positive' : 'negative'}`}>
          {formatCurrency(capGains)}
        </div>
        <div className="stat-sub">Long-term + Short-term</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">PII Entities Detected</div>
        <div className="stat-value">{piiStats.total_entities_detected || 0}</div>
        <div className="stat-sub">{Object.keys(piiStats.entity_counts || {}).length} types identified</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Ending Capital</div>
        <div className="stat-value">{formatCurrency(k1.capital_account_ending)}</div>
        <div className="stat-sub">Partner capital account</div>
      </div>
    </div>
  )
}

function FinancialDataCard({ k1, mapping }) {
  if (!k1) return null

  const rows = [
    ['Ordinary Business Income', k1.ordinary_business_income],
    ['Guaranteed Payments', k1.guaranteed_payments],
    ['Interest Income', k1.interest_income],
    ['Ordinary Dividends', k1.ordinary_dividends],
    ['Qualified Dividends', k1.qualified_dividends],
    ['Net Short-Term Capital Gain', k1.short_term_capital_gains],
    ['Net Long-Term Capital Gain', k1.long_term_capital_gains],
    ['Rental Real Estate Income', k1.rental_real_estate_income],
    ['Section 179 Deduction', k1.section_179_deduction],
    ['Self-Employment Earnings', k1.self_employment_earnings],
    ['Foreign Taxes Paid', k1.foreign_taxes_paid],
    ['QBI Deduction', k1.qbi_deduction],
    ['Distributions', k1.distributions],
  ].filter(([, v]) => v != null)

  return (
    <div className="card">
      <div className="card-header">
        <h2>Extracted K-1 Financial Data</h2>
        <span className="tag financial">AI Extracted</span>
      </div>
      <div className="card-body">
        {k1.partnership_name && (
          <div style={{ marginBottom: '1rem', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
            <strong>Partnership:</strong> {resolvePlaceholders(k1.partnership_name, mapping)}
            {k1.partner_type && <> &middot; <strong>Role:</strong> {k1.partner_type}</>}
            {k1.partner_share_percentage != null && <> &middot; <strong>Share:</strong> {k1.partner_share_percentage}%</>}
          </div>
        )}
        <table className="financial-table">
          <tbody>
            {rows.map(([label, value]) => (
              <tr key={label}>
                <td>{label}</td>
                <td className={value < 0 ? 'amount-negative' : 'amount-positive'}>
                  {formatCurrency(value)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {k1.capital_account_beginning != null && (
          <div style={{ marginTop: '1.25rem' }}>
            <h3 style={{ fontSize: '0.75rem', fontWeight: 600, marginBottom: '0.65rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Capital Account Movement
            </h3>
            <CapitalAccountBar k1={k1} />
          </div>
        )}
      </div>
    </div>
  )
}

function CapitalAccountBar({ k1 }) {
  const beginning = k1.capital_account_beginning || 0
  const contributed = k1.capital_contributed || 0
  const increase = k1.current_year_increase || k1.ordinary_business_income || 0
  const distributions = Math.abs(k1.distributions || 0)
  const ending = k1.capital_account_ending || 0

  const total = beginning + contributed + increase
  if (total === 0) return null

  const segments = [
    { label: 'Beginning', value: beginning, color: 'var(--navy)' },
    { label: 'Contributed', value: contributed, color: 'var(--navy-500)' },
    { label: 'Increase', value: increase, color: 'var(--navy-400)' },
  ]

  return (
    <>
      <div className="capital-bar-track">
        {segments.map((seg) => (
          <div key={seg.label} className="capital-bar-segment" style={{ width: `${(seg.value / total) * 100}%`, background: seg.color }}>
            {seg.value > 0 ? formatCurrency(seg.value) : ''}
          </div>
        ))}
      </div>
      <div className="capital-legend">
        {segments.map((seg) => (
          <div key={seg.label} className="capital-legend-item">
            <div className="capital-legend-dot" style={{ background: seg.color }} />
            {seg.label}: {formatCurrency(seg.value)}
          </div>
        ))}
        <div className="capital-legend-item">
          <div className="capital-legend-dot" style={{ background: 'var(--negative)' }} />
          Distributions: -{formatCurrency(distributions)}
        </div>
        <div className="capital-legend-item" style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
          Ending: {formatCurrency(ending)}
        </div>
      </div>
    </>
  )
}

function AnalysisCard({ analysis }) {
  if (!analysis) return null

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>AI Financial Analysis</h2>
        <span className="tag ai">DeepSeek</span>
      </div>
      <div className="card-body">
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
          <div>
            <div className="analysis-section-heading">Key Observations</div>
            <div className="analysis-list">
              {(analysis.key_observations || []).map((obs, i) => (
                <div key={i} className="analysis-item">{obs}</div>
              ))}
            </div>
          </div>
          <div>
            <div className="analysis-section-heading">Tax Planning Recommendations</div>
            <div className="analysis-list">
              {(analysis.tax_planning_recommendations || []).map((rec, i) => (
                <div key={i} className="analysis-item">{rec}</div>
              ))}
            </div>
          </div>
        </div>
        {(analysis.total_income != null || analysis.net_taxable_income != null) && (
          <div className="analysis-totals">
            <div className="meta-item">
              <div className="meta-label">Total Income</div>
              <div className="meta-value" style={{ color: 'var(--positive)' }}>{formatCurrency(analysis.total_income)}</div>
            </div>
            <div className="meta-item">
              <div className="meta-label">Total Deductions</div>
              <div className="meta-value" style={{ color: 'var(--negative)' }}>{formatCurrency(analysis.total_deductions)}</div>
            </div>
            <div className="meta-item">
              <div className="meta-label">Net Taxable</div>
              <div className="meta-value">{formatCurrency(analysis.net_taxable_income)}</div>
            </div>
            <div className="meta-item">
              <div className="meta-label">Distribution Ratio</div>
              <div className="meta-value">{analysis.distribution_vs_income_ratio || 'N/A'}</div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function PipelineSteps({ metadata }) {
  const steps = [
    { key: 'ingestion_timestamp', label: 'PDF Ingestion', desc: 'Read raw PDF from S3' },
    { key: 'ocr_timestamp', label: 'OCR Extraction', desc: 'Surya layout-aware OCR' },
    { key: 'pii_scan_timestamp', label: 'PII Detection', desc: 'Presidio + GLiNER scan' },
    { key: 'sanitization_timestamp', label: 'Text Sanitization', desc: 'Replace PII with placeholders' },
    { key: 'extraction_timestamp', label: 'AI Extraction', desc: 'Structured K-1 field extraction' },
    { key: 'analysis_timestamp', label: 'AI Analysis', desc: 'Financial analysis & recommendations' },
    { key: 'deterministic_validation_timestamp', label: 'Rule Validation', desc: 'Deterministic checks (30+ rules)' },
    { key: 'ai_validation_timestamp', label: 'AI Validation', desc: 'Quality & coherence assessment' },
    { key: 'report_generated_at', label: 'Report Generation', desc: 'Final deliverables (JSON, CSV, PDF)' },
  ]

  return (
    <div className="card full-width" style={{ marginBottom: '2rem' }}>
      <div className="card-header">
        <h2>Pipeline Steps</h2>
        <span className="tag financial">{steps.filter(s => metadata[s.key]).length} / {steps.length} completed</span>
      </div>
      <div className="card-body">
        <div className="pipeline-steps">
          {steps.map((step, i) => {
            const ts = metadata[step.key]
            const completed = !!ts
            return (
              <div key={step.key} className={`pipeline-step ${completed ? 'completed' : 'pending'}`}>
                <div className="pipeline-step-indicator">
                  <div className="pipeline-step-dot">{completed ? '\u2713' : (i + 1)}</div>
                  {i < steps.length - 1 && <div className="pipeline-step-line" />}
                </div>
                <div className="pipeline-step-content">
                  <div className="pipeline-step-label">{step.label}</div>
                  <div className="pipeline-step-desc">{step.desc}</div>
                  {ts && <div className="pipeline-step-time">{new Date(ts).toLocaleString()}</div>}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function SummaryTab({ data, mapping }) {
  const pr = data.pipeline_results || {}
  return (
    <>
      <PipelineSteps metadata={pr.processing_metadata || {}} />
      <StatsGrid data={pr} mapping={mapping} />
      <div className="main-grid">
        <FinancialDataCard k1={pr.k1_data} mapping={mapping} />
        <AnalysisCard analysis={pr.financial_analysis} />
      </div>
    </>
  )
}

// ================================================================
//  Tab: Validation
// ================================================================

function CrossPartnerSection({ crossPartner }) {
  if (!crossPartner || !crossPartner.summary) return null

  const summary = crossPartner.summary
  const results = crossPartner.results || []
  const partnerships = crossPartner.partnerships_validated || []
  const failed = results.filter(r => !r.passed)

  const [cpFilter, setCpFilter] = useState('all')
  const filteredFailed = cpFilter === 'all' ? failed : failed.filter(r => r.severity === cpFilter)

  const hasCritical = summary.critical > 0
  const hasWarnings = summary.warnings > 0
  const borderColor = hasCritical ? 'var(--negative)' : hasWarnings ? '#856404' : 'var(--positive)'
  const statusLabel = hasCritical ? 'Cross-Partner Issues Found' : hasWarnings ? 'Cross-Partner Warnings' : 'Cross-Partner Checks Passed'

  return (
    <>
      <div className="card full-width" style={{ borderLeft: `4px solid ${borderColor}`, marginTop: '1.5rem' }}>
        <div className="card-body" style={{ padding: '1rem 1.25rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ fontSize: '1rem', fontWeight: 700, color: borderColor }}>
              {statusLabel}
            </div>
            <div style={{ display: 'flex', gap: '1.5rem', fontSize: '0.85rem' }}>
              <span><strong>{summary.total_checks}</strong> checks</span>
              <span style={{ color: 'var(--negative)' }}><strong>{summary.critical}</strong> critical</span>
              <span style={{ color: '#856404' }}><strong>{summary.warnings}</strong> warnings</span>
              <span style={{ color: 'var(--text-muted)' }}><strong>{summary.advisory}</strong> advisory</span>
            </div>
          </div>
        </div>
      </div>

      {partnerships.length > 0 && (
        <div className="card full-width">
          <div className="card-header">
            <h2>Partnerships Validated</h2>
            <span className="tag financial">{partnerships.length} partnerships</span>
          </div>
          <div className="card-body">
            <table className="financial-table">
              <thead>
                <tr>
                  <th>Partnership</th>
                  <th>Year</th>
                  <th style={{ textAlign: 'center' }}>Partners</th>
                </tr>
              </thead>
              <tbody>
                {partnerships.map((p, i) => (
                  <tr key={i}>
                    <td>{p.partnership_name || p.partnership_ein}</td>
                    <td>{p.tax_year}</td>
                    <td style={{ textAlign: 'center' }}>{p.partner_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {crossPartner.year_pairs_checked > 0 && (
              <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '0.5rem' }}>
                {crossPartner.year_pairs_checked} consecutive year pair(s) checked for continuity
              </p>
            )}
          </div>
        </div>
      )}

      {failed.length > 0 && (
        <div className="card full-width">
          <div className="card-header">
            <h2>Cross-Partner Issues</h2>
            <div style={{ display: 'flex', gap: '4px' }}>
              {['all', 'critical', 'warning', 'advisory'].map(s => (
                <button
                  key={s}
                  className={`tab-btn ${cpFilter === s ? 'active' : ''}`}
                  onClick={() => setCpFilter(s)}
                  style={{ fontSize: '0.75rem', padding: '4px 10px' }}
                >
                  {s === 'all' ? `All (${failed.length})` : `${s} (${failed.filter(r => r.severity === s).length})`}
                </button>
              ))}
            </div>
          </div>
          <div className="card-body">
            {filteredFailed.length === 0 ? (
              <p style={{ color: 'var(--text-muted)' }}>No {cpFilter} issues found.</p>
            ) : (
              <table className="financial-table">
                <thead>
                  <tr>
                    <th>Rule</th>
                    <th>Category</th>
                    <th>Severity</th>
                    <th>Message</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredFailed.map((r, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: 'monospace', fontWeight: 600 }}>{r.rule_id}</td>
                      <td>{r.category}</td>
                      <td>
                        <span className={`tag ${r.severity === 'critical' ? 'compliance' : r.severity === 'warning' ? 'financial' : ''}`}>
                          {r.severity}
                        </span>
                      </td>
                      <td style={{ fontSize: '0.8rem' }}>{r.message}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </>
  )
}

function ValidationTab({ data, crossPartnerData }) {
  const pr = data.pipeline_results || {}
  const validation = pr.validation

  if (!validation) {
    return (
      <div className="card full-width">
        <div className="card-body">
          <p style={{ color: 'var(--text-muted)' }}>No validation data available. Re-run the pipeline to generate validation results.</p>
        </div>
      </div>
    )
  }

  const det = validation.deterministic || {}
  const ai = validation.ai || {}
  const overall = validation.overall_status || 'pending'
  const checks = det.checks || []
  const failed = checks.filter(c => !c.passed)

  const [severityFilter, setSeverityFilter] = useState('all')
  const filteredFailed = severityFilter === 'all' ? failed : failed.filter(c => c.severity === severityFilter)

  const statusColors = { passed: 'var(--positive)', warnings: '#856404', failed: 'var(--negative)' }
  const statusLabels = { passed: 'All Checks Passed', warnings: 'Warnings Detected', failed: 'Critical Failures' }

  const scoreBar = (score) => {
    const pct = Math.round(score * 100)
    const color = score >= 0.7 ? 'var(--positive)' : score >= 0.5 ? '#856404' : 'var(--negative)'
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <span style={{ fontWeight: 600, minWidth: '36px' }}>{pct}%</span>
        <div style={{ flex: 1, height: '8px', background: 'var(--border)', borderRadius: '4px', overflow: 'hidden' }}>
          <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
        </div>
      </div>
    )
  }

  return (
    <>
      <div className="card full-width" style={{ borderLeft: `4px solid ${statusColors[overall] || 'var(--border)'}` }}>
        <div className="card-body" style={{ padding: '1rem 1.25rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ fontSize: '1.1rem', fontWeight: 700, color: statusColors[overall] }}>
              {statusLabels[overall] || overall}
            </div>
            <div style={{ display: 'flex', gap: '1.5rem', fontSize: '0.85rem' }}>
              <span><strong>{checks.length}</strong> checks run</span>
              <span style={{ color: 'var(--negative)' }}><strong>{det.critical_count || 0}</strong> critical</span>
              <span style={{ color: '#856404' }}><strong>{det.warning_count || 0}</strong> warnings</span>
              <span style={{ color: 'var(--text-muted)' }}><strong>{det.advisory_count || 0}</strong> advisory</span>
            </div>
          </div>
        </div>
      </div>

      <div className="main-grid">
        <div className="card full-width">
          <div className="card-header">
            <h2>Deterministic Checks</h2>
            <div style={{ display: 'flex', gap: '4px' }}>
              {['all', 'critical', 'warning', 'advisory'].map(s => (
                <button
                  key={s}
                  className={`tab-btn ${severityFilter === s ? 'active' : ''}`}
                  onClick={() => setSeverityFilter(s)}
                  style={{ fontSize: '0.75rem', padding: '4px 10px' }}
                >
                  {s === 'all' ? `All (${failed.length})` : `${s} (${failed.filter(c => c.severity === s).length})`}
                </button>
              ))}
            </div>
          </div>
          <div className="card-body">
            {filteredFailed.length === 0 ? (
              <p style={{ color: 'var(--text-muted)' }}>
                {severityFilter === 'all' ? 'All deterministic checks passed.' : `No ${severityFilter} issues found.`}
              </p>
            ) : (
              <table className="financial-table">
                <thead>
                  <tr>
                    <th>Rule</th>
                    <th>Name</th>
                    <th>Severity</th>
                    <th>Message</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredFailed.map((c, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: 'monospace', fontWeight: 600 }}>{c.rule_id}</td>
                      <td>{c.rule_name}</td>
                      <td>
                        <span className={`tag ${c.severity === 'critical' ? 'compliance' : c.severity === 'warning' ? 'financial' : ''}`}>
                          {c.severity}
                        </span>
                      </td>
                      <td style={{ fontSize: '0.8rem' }}>{c.message}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {ai.overall_coherence_score != null && (
          <div className="card full-width">
            <div className="card-header">
              <h2>AI Quality Assessment</h2>
              <span className="tag ai">DeepSeek</span>
            </div>
            <div className="card-body">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem', marginBottom: '1.5rem' }}>
                <div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: '4px' }}>Coherence Score</div>
                  {scoreBar(ai.overall_coherence_score)}
                </div>
                <div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: '4px' }}>OCR Confidence</div>
                  {scoreBar(ai.ocr_confidence_score)}
                </div>
              </div>

              {ai.partnership_type_assessment && (
                <div style={{ marginBottom: '1rem' }}>
                  <strong>Partnership Type:</strong> {ai.partnership_type_assessment}
                  {ai.partnership_type_consistency != null && (
                    <span style={{ marginLeft: '8px', color: 'var(--text-muted)' }}>
                      ({Math.round(ai.partnership_type_consistency * 100)}% consistency)
                    </span>
                  )}
                </div>
              )}

              {ai.anomaly_flags && ai.anomaly_flags.length > 0 && (
                <div style={{ marginBottom: '1rem' }}>
                  <div className="analysis-section-heading">Anomaly Flags</div>
                  <div className="analysis-list">
                    {ai.anomaly_flags.map((a, i) => (
                      <div key={i} className="analysis-item">
                        <strong>{a.field_name}</strong>: {a.description}
                        <span style={{ marginLeft: '8px', color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                          ({Math.round(a.confidence * 100)}% confidence)
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {ai.narrative_assessment && (
                <div style={{ marginBottom: '1rem' }}>
                  <div className="analysis-section-heading">Narrative Assessment</div>
                  <p style={{ fontSize: '0.85rem', lineHeight: '1.6' }}>{ai.narrative_assessment}</p>
                </div>
              )}

              {ai.recommended_review_fields && ai.recommended_review_fields.length > 0 && (
                <div>
                  <div className="analysis-section-heading">Recommended for Review</div>
                  <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                    {ai.recommended_review_fields.map((f, i) => (
                      <span key={i} className="tag compliance">{f}</span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      <CrossPartnerSection crossPartner={crossPartnerData} />
    </>
  )
}

// ================================================================
//  Tab 2: PII & Redactions
// ================================================================

function PIICard({ piiStats }) {
  if (!piiStats) return null
  const entityTypes = piiStats.entity_counts || {}
  const sortedTypes = Object.entries(entityTypes).sort((a, b) => b[1] - a[1])

  return (
    <div className="card">
      <div className="card-header">
        <h2>PII Detection Report</h2>
        <span className="tag compliance">Compliance</span>
      </div>
      <div className="card-body">
        <div className="pii-entities">
          {sortedTypes.map(([type, count]) => (
            <div key={type} className="pii-entity">
              <span className="pii-entity-type">{type.replace(/_/g, ' ')}</span>
              <span className="pii-entity-count">{count}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function PIIComparisonCard({ comparison }) {
  if (!comparison) return null

  const modes = [
    { key: 'presidio_only', label: 'Presidio', color: 'var(--navy)' },
    { key: 'gliner_only', label: 'GLiNER', color: 'var(--navy-500)' },
    { key: 'combined', label: 'Combined', color: 'var(--navy-400)' },
  ]

  const allTypes = [...new Set(
    modes.flatMap(m => Object.keys(comparison[m.key]?.counts || {}))
  )].sort()

  const maxCount = Math.max(...modes.map(m => comparison[m.key]?.total || 0), 1)

  const getEntities = (modeKey) => {
    return (comparison[modeKey]?.entities || [])
      .filter(e => e.score >= 0.4)
      .sort((a, b) => b.score - a.score)
  }

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>PII Detection — Model Comparison</h2>
        <span className="tag compliance">Presidio vs GLiNER</span>
      </div>
      <div className="card-body">
        <div className="comp-totals">
          {modes.map(m => (
            <div key={m.key} className="comp-total-row">
              <div className="comp-total-label">{m.label}</div>
              <div className="comp-total-bar-track">
                <div className="comp-total-bar-fill" style={{ width: `${((comparison[m.key]?.total || 0) / maxCount) * 100}%`, background: m.color }} />
              </div>
              <div className="comp-total-count">{comparison[m.key]?.total || 0}</div>
            </div>
          ))}
        </div>

        <table className="comp-table">
          <thead>
            <tr>
              <th>Entity Type</th>
              {modes.map(m => <th key={m.key}>{m.label}</th>)}
            </tr>
          </thead>
          <tbody>
            {allTypes.map(type => {
              const cells = modes.map(m => comparison[m.key]?.counts?.[type] || 0)
              const rowMax = Math.max(...cells, 1)
              return (
                <tr key={type}>
                  <td className="comp-type-cell">{type.replace(/_/g, ' ')}</td>
                  {modes.map((m, i) => (
                    <td key={m.key}>
                      <div className="comp-cell">
                        <div className="comp-cell-bar-track">
                          <div className="comp-cell-bar-fill" style={{ width: cells[i] > 0 ? `${(cells[i] / rowMax) * 100}%` : '0%', background: m.color }} />
                        </div>
                        <span className="comp-cell-count">{cells[i]}</span>
                      </div>
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>

        <div className="comp-details">
          {modes.map(m => {
            const entities = getEntities(m.key)
            return (
              <div key={m.key} className="comp-detail-col">
                <div className="comp-detail-heading">{m.label} — {entities.length} high-confidence</div>
                <div className="comp-snippet-list">
                  {entities.map((e, i) => (
                    <div key={i} className="comp-snippet" style={{ borderLeftColor: m.color }}>
                      <span className="comp-snippet-type">{e.entity_type}</span>
                      <code className="comp-snippet-text">{e.text_snippet.replace(/\n/g, ' ').slice(0, 40)}</code>
                      <span className="comp-snippet-score">{(e.score * 100).toFixed(0)}%</span>
                    </div>
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function PlaceholderMappingCard({ mapping }) {
  if (!mapping) return null
  const entries = Object.entries(mapping)

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>PII Placeholder Mapping</h2>
        <span className="tag compliance">{entries.length} unique entities</span>
      </div>
      <div className="card-body">
        <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '0.75rem' }}>
          Each unique PII value gets a numbered placeholder. This mapping enables reversibility.
        </p>
        <table className="mapping-table">
          <thead>
            <tr>
              <th>Placeholder</th>
              <th>Original Value</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([placeholder, original]) => (
              <tr key={placeholder}>
                <td><code className="placeholder-code">{placeholder}</code></td>
                <td className="original-value">{original.replace(/\n/g, ' ')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function PIITab({ data }) {
  const pr = data.pipeline_results || {}
  return (
    <div className="main-grid">
      <PIICard piiStats={pr.pii_stats} />
      <PlaceholderMappingCard mapping={data.placeholder_mapping} />
      <PIIComparisonCard comparison={pr.pii_comparison} />
    </div>
  )
}

// ================================================================
//  Tab 3: AI Audit
// ================================================================

function AIInteractionSection({ title, description, interaction }) {
  if (!interaction) return null

  const [expandedPrompt, setExpandedPrompt] = useState(null)

  const toggle = (key) => setExpandedPrompt(prev => prev === key ? null : key)

  // Parse the response from raw_messages
  const responseMsg = interaction.raw_messages?.find(m => m.role === 'response')
  const toolCallArgs = responseMsg?.tool_calls?.[0]?.args
  let parsedResponse = null
  if (toolCallArgs) {
    try { parsedResponse = JSON.parse(toolCallArgs) } catch { parsedResponse = toolCallArgs }
  }

  // Format usage stats if available
  const usageText = interaction.usage
    ? (typeof interaction.usage === 'object'
      ? `${interaction.usage.request_tokens || interaction.usage.requests || '?'} req / ${interaction.usage.response_tokens || '?'} resp tokens`
      : String(interaction.usage))
    : null

  return (
    <div className="ai-section">
      <h3 className="ai-section-title">{title}</h3>
      {description && <p className="ai-section-desc">{description}</p>}
      <div className="ai-meta-row">
        <span className="ai-meta-badge">{interaction.model || 'unknown'}</span>
        {usageText && <span className="ai-meta-usage">{usageText}</span>}
      </div>

      <div className="ai-prompt-group">
        <button className="ai-prompt-toggle" onClick={() => toggle('system')}>
          {expandedPrompt === 'system' ? '[-]' : '[+]'} System Prompt
        </button>
        {expandedPrompt === 'system' && (
          <pre className="ai-prompt-block">{interaction.system_prompt}</pre>
        )}
      </div>

      <div className="ai-prompt-group">
        <button className="ai-prompt-toggle" onClick={() => toggle('user')}>
          {expandedPrompt === 'user' ? '[-]' : '[+]'} User Prompt
        </button>
        {expandedPrompt === 'user' && (
          <pre className="ai-prompt-block">{interaction.user_prompt}</pre>
        )}
      </div>

      {interaction.output_schema && (
        <div className="ai-prompt-group">
          <button className="ai-prompt-toggle" onClick={() => toggle('schema')}>
            {expandedPrompt === 'schema' ? '[-]' : '[+]'} Output Schema ({interaction.output_schema.title})
          </button>
          {expandedPrompt === 'schema' && (
            <pre className="ai-prompt-block">{JSON.stringify(interaction.output_schema, null, 2)}</pre>
          )}
        </div>
      )}

      {parsedResponse && (
        <div className="ai-prompt-group">
          <button className="ai-prompt-toggle" onClick={() => toggle('response')}>
            {expandedPrompt === 'response' ? '[-]' : '[+]'} Model Response
          </button>
          {expandedPrompt === 'response' && (
            <pre className="ai-prompt-block">{typeof parsedResponse === 'string' ? parsedResponse : JSON.stringify(parsedResponse, null, 2)}</pre>
          )}
        </div>
      )}
    </div>
  )
}

function AIAuditTab({ data }) {
  const ai = data.ai_interactions
  if (!ai) return <div className="card full-width"><div className="card-body"><p style={{ color: 'var(--text-muted)' }}>No AI interaction data available (k1_report.json not found in output).</p></div></div>

  const steps = [
    { key: 'extraction', title: 'Step 1: Structured Data Extraction', desc: 'Extract K-1 financial fields from sanitized OCR text' },
    { key: 'analysis', title: 'Step 2: Financial Analysis', desc: 'Analyze extracted data for income totals, observations, and tax recommendations' },
    { key: 'validation', title: 'Step 3: AI Quality Validation', desc: 'Assess data coherence, OCR confidence, and flag anomalies' },
  ]

  const availableSteps = steps.filter(s => ai[s.key])

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>AI Interaction Audit Trail</h2>
        <span className="tag ai">{availableSteps.length} AI steps</span>
      </div>
      <div className="card-body">
        {steps.map(step => (
          ai[step.key] ? (
            <AIInteractionSection key={step.key} title={step.title} description={step.desc} interaction={ai[step.key]} />
          ) : null
        ))}
        {availableSteps.length === 0 && (
          <p style={{ color: 'var(--text-muted)' }}>No AI interaction data recorded.</p>
        )}
      </div>
    </div>
  )
}

// ================================================================
//  Tab 4: OCR Text
// ================================================================

function OCRTab({ data }) {
  const [view, setView] = useState('side-by-side')
  const ocrText = data.ocr_text
  const sanitizedText = data.sanitized_text

  const rawRef = useRef(null)
  const sanitizedRef = useRef(null)
  const syncing = useRef(false)

  const handleScroll = useCallback((source, target) => {
    if (syncing.current || !source.current || !target.current) return
    syncing.current = true
    target.current.scrollTop = source.current.scrollTop
    target.current.scrollLeft = source.current.scrollLeft
    syncing.current = false
  }, [])

  if (!ocrText) {
    return (
      <div className="card full-width">
        <div className="card-body">
          <p style={{ color: 'var(--text-muted)' }}>No OCR text available (staging data not found).</p>
        </div>
      </div>
    )
  }

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>OCR Text</h2>
        <div className="ocr-view-toggle">
          <button className={view === 'raw' ? 'active' : ''} onClick={() => setView('raw')}>Raw</button>
          <button className={view === 'sanitized' ? 'active' : ''} onClick={() => setView('sanitized')}>Sanitized</button>
          <button className={view === 'side-by-side' ? 'active' : ''} onClick={() => setView('side-by-side')}>Side by Side</button>
        </div>
      </div>
      <div className="card-body">
        {view === 'side-by-side' ? (
          <div className="ocr-side-by-side">
            <div className="ocr-panel">
              <div className="ocr-panel-label">Raw OCR Text</div>
              <pre className="ocr-text" ref={rawRef} onScroll={() => handleScroll(rawRef, sanitizedRef)}>{ocrText}</pre>
            </div>
            <div className="ocr-panel">
              <div className="ocr-panel-label">Sanitized (PII Redacted)</div>
              <pre className="ocr-text sanitized" ref={sanitizedRef} onScroll={() => handleScroll(sanitizedRef, rawRef)}>{sanitizedText || 'Not available'}</pre>
            </div>
          </div>
        ) : (
          <pre className="ocr-text">{view === 'raw' ? ocrText : (sanitizedText || 'Not available')}</pre>
        )}
      </div>
    </div>
  )
}

// ================================================================
//  Tab 5: Metadata
// ================================================================

function MetadataTab({ data }) {
  const pr = data.pipeline_results || {}
  const metadata = pr.processing_metadata || {}

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>Processing Metadata</h2>
      </div>
      <div className="card-body">
        <div className="processing-meta">
          {Object.entries(metadata).map(([key, value]) => (
            <div key={key} className="meta-item">
              <div className="meta-label">{key.replace(/_/g, ' ')}</div>
              <div className="meta-value">{value ? new Date(value).toLocaleString() : 'N/A'}</div>
            </div>
          ))}
        </div>
        {pr.output_files && (
          <>
            <h3 style={{ fontSize: '0.75rem', fontWeight: 600, margin: '1.25rem 0 0.65rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Output Files
            </h3>
            <div className="processing-meta">
              {Object.entries(pr.output_files).map(([key, value]) => (
                <div key={key} className="meta-item">
                  <div className="meta-label">{key.replace(/_/g, ' ')}</div>
                  <div className="meta-value" style={{ fontSize: '0.75rem', wordBreak: 'break-all' }}>{value}</div>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

// ================================================================
//  Report Detail
// ================================================================

function ReportDetail({ dirName, onBack }) {
  const [data, setData] = useState(null)
  const [crossPartnerData, setCrossPartnerData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [tab, setTab] = useState('summary')

  useEffect(() => {
    Promise.all([
      fetch(`/api/reports/${encodeURIComponent(dirName)}`).then(r => r.json()),
      fetch('/api/cross-partner').then(r => r.json()).catch(() => null),
    ]).then(([reportData, cpData]) => {
      setData(reportData)
      setCrossPartnerData(cpData)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [dirName])

  if (loading) return <div className="loading"><div className="loading-spinner" /><p>Loading report...</p></div>
  if (!data) return <div className="error-state"><h2>Failed to load report</h2></div>

  const mapping = data.placeholder_mapping
  const pr = data.pipeline_results || {}
  const displayName = resolvePlaceholders(pr.k1_data?.partnership_name, mapping) || cleanDirName(dirName)

  return (
    <>
      <button className="back-btn" onClick={onBack}>Back to reports</button>
      <div className="report-detail-header">
        <h2>{displayName}</h2>
        {pr.k1_data?.tax_year && <span className="tag financial">Tax Year {pr.k1_data.tax_year}</span>}
      </div>
      <TabNav active={tab} onChange={setTab} />
      {tab === 'summary' && <SummaryTab data={data} mapping={mapping} />}
      {tab === 'validation' && <ValidationTab data={data} crossPartnerData={filterCrossPartnerForReport(pr.cross_partner_validation || crossPartnerData, mapping)} />}
      {tab === 'pii' && <PIITab data={data} />}
      {tab === 'ai' && <AIAuditTab data={data} />}
      {tab === 'ocr' && <OCRTab data={data} />}
      {tab === 'meta' && <MetadataTab data={data} />}
    </>
  )
}

// ================================================================
//  App
// ================================================================

function App() {
  const [selectedReport, setSelectedReport] = useState(null)

  return (
    <div className="app">
      <div className="header">
        <div className="header-left">
          <div className="header-icon">K-1</div>
          <div>
            <h1>K-1 Document Intelligence</h1>
            <p>Pipeline audit dashboard</p>
          </div>
        </div>
      </div>

      {selectedReport
        ? <ReportDetail dirName={selectedReport} onBack={() => setSelectedReport(null)} />
        : <>
            <CrossPartnerDashboard />
            <ReportList onSelect={setSelectedReport} />
          </>
      }
    </div>
  )
}

export default App
