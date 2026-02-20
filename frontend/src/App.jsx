import { useState, useEffect } from 'react'
import './App.css'

const formatCurrency = (value) => {
  if (value == null) return '$0'
  const num = typeof value === 'string' ? parseFloat(value) : value
  const sign = num < 0 ? '-' : ''
  return `${sign}$${Math.abs(num).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`
}

const PipelineSteps = () => {
  const steps = [
    'PDF Ingestion',
    'OCR Extraction',
    'PII Detection',
    'Sanitization',
    'AI Extraction',
    'Financial Analysis',
    'Report',
  ]
  return (
    <div className="pipeline-steps">
      {steps.map((label, i) => (
        <span key={i} style={{ display: 'contents' }}>
          <div className="pipeline-step active">
            <span className="step-num">{i + 1}</span>
            {label}
          </div>
          {i < steps.length - 1 && <span className="pipeline-arrow">/</span>}
        </span>
      ))}
    </div>
  )
}

const StatsGrid = ({ data }) => {
  const k1 = data.structured_k1 || data.k1_data || {}
  const analysis = data.financial_analysis || {}
  const piiStats = data.pii_stats || {}

  const capGains = (k1.net_long_term_capital_gain ?? k1.long_term_capital_gains ?? 0) +
    (k1.net_short_term_capital_gain ?? k1.short_term_capital_gains ?? 0)

  return (
    <div className="stats-grid">
      <div className="stat-card">
        <div className="stat-label">Total Income</div>
        <div className={`stat-value ${(analysis.total_income || k1.ordinary_business_income || 0) >= 0 ? 'positive' : 'negative'}`}>
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
        <div className="stat-value">{piiStats.total_entities || piiStats.total_entities_detected || 0}</div>
        <div className="stat-sub">{Object.keys(piiStats.entity_types || piiStats.entity_counts || {}).length} types identified</div>
      </div>
      <div className="stat-card">
        <div className="stat-label">Ending Capital</div>
        <div className="stat-value">
          {formatCurrency(k1.capital_account_ending || k1.ending_capital_account)}
        </div>
        <div className="stat-sub">Partner capital account</div>
      </div>
    </div>
  )
}

const FinancialDataCard = ({ k1 }) => {
  if (!k1) return null

  const rows = [
    ['Ordinary Business Income', k1.ordinary_business_income],
    ['Guaranteed Payments', k1.guaranteed_payments],
    ['Interest Income', k1.interest_income],
    ['Ordinary Dividends', k1.ordinary_dividends],
    ['Qualified Dividends', k1.qualified_dividends],
    ['Net Short-Term Capital Gain', k1.net_short_term_capital_gain ?? k1.short_term_capital_gains],
    ['Net Long-Term Capital Gain', k1.net_long_term_capital_gain ?? k1.long_term_capital_gains],
    ['Rental Real Estate Income', k1.rental_real_estate_income ?? k1.net_rental_real_estate_income],
    ['Section 179 Deduction', k1.section_179_deduction],
    ['Distributions', k1.distributions],
  ].filter(([, v]) => v != null)

  return (
    <div className="card">
      <div className="card-header">
        <h2>Extracted K-1 Financial Data</h2>
        <span className="tag financial">AI Extracted</span>
      </div>
      <div className="card-body">
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
        {(k1.capital_account_beginning != null || k1.beginning_capital_account != null) && (
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

const CapitalAccountBar = ({ k1 }) => {
  const beginning = k1.capital_account_beginning || k1.beginning_capital_account || 0
  const contributed = k1.capital_contributed || 0
  const increase = k1.current_year_increase || k1.ordinary_business_income || 0
  const distributions = Math.abs(k1.distributions || 0)
  const ending = k1.capital_account_ending || k1.ending_capital_account || 0

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
          <div
            key={seg.label}
            className="capital-bar-segment"
            style={{
              width: `${(seg.value / total) * 100}%`,
              background: seg.color,
            }}
          >
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

const PIICard = ({ piiStats }) => {
  if (!piiStats) return null

  const entityTypes = piiStats.entity_types || piiStats.entity_counts || {}
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
          {sortedTypes.length === 0 && (
            <p style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>No PII entities detected</p>
          )}
        </div>
      </div>
    </div>
  )
}

const PIIComparisonCard = ({ comparison }) => {
  if (!comparison) return null

  const modes = [
    { key: 'presidio_only', label: 'Presidio', color: 'var(--navy)' },
    { key: 'gliner_only', label: 'GLiNER', color: 'var(--navy-500)' },
    { key: 'combined', label: 'Combined', color: 'var(--navy-400)' },
  ]

  const allTypes = [...new Set(
    modes.flatMap(m => Object.keys(comparison[m.key]?.counts || {}))
  )].sort()

  const maxCount = Math.max(
    ...modes.map(m => comparison[m.key]?.total || 0), 1
  )

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
                <div
                  className="comp-total-bar-fill"
                  style={{
                    width: `${((comparison[m.key]?.total || 0) / maxCount) * 100}%`,
                    background: m.color,
                  }}
                />
              </div>
              <div className="comp-total-count">
                {comparison[m.key]?.total || 0}
              </div>
            </div>
          ))}
        </div>

        <table className="comp-table">
          <thead>
            <tr>
              <th>Entity Type</th>
              {modes.map(m => (
                <th key={m.key}>{m.label}</th>
              ))}
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
                          <div
                            className="comp-cell-bar-fill"
                            style={{
                              width: cells[i] > 0 ? `${(cells[i] / rowMax) * 100}%` : '0%',
                              background: m.color,
                            }}
                          />
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
                <div className="comp-detail-heading">
                  {m.label} — {entities.length} high-confidence
                </div>
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

const AnalysisCard = ({ analysis }) => {
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

const ProcessingMeta = ({ metadata }) => {
  if (!metadata) return null

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>Processing Metadata</h2>
      </div>
      <div className="card-body">
        <div className="processing-meta">
          <div className="meta-item">
            <div className="meta-label">Source Document</div>
            <div className="meta-value">{metadata.source_file || 'K-1 Schedule'}</div>
          </div>
          <div className="meta-item">
            <div className="meta-label">Pages Processed</div>
            <div className="meta-value">{metadata.page_count || 'N/A'}</div>
          </div>
          <div className="meta-item">
            <div className="meta-label">Processing Date</div>
            <div className="meta-value">{(metadata.processed_at || metadata.report_generated_at) ? new Date(metadata.processed_at || metadata.report_generated_at).toLocaleDateString() : 'N/A'}</div>
          </div>
        </div>
      </div>
    </div>
  )
}

/* ================================================================
   Batch Mode Components
   ================================================================ */

const BatchOverviewCard = ({ batchData }) => {
  if (!batchData) return null

  const profiles = batchData.profiles || []
  const successful = profiles.filter(p => p.status === 'success')

  return (
    <div className="card full-width">
      <div className="card-header">
        <h2>Batch Processing Overview</h2>
        <span className="tag financial">{successful.length}/{profiles.length} Processed</span>
      </div>
      <div className="card-body">
        <table className="batch-overview-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Partnership</th>
              <th>Entity</th>
              <th>Role</th>
              <th style={{ textAlign: 'right' }}>Net Income</th>
              <th style={{ textAlign: 'right' }}>Capital End</th>
              <th style={{ textAlign: 'center' }}>PII</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {profiles.map((p) => {
              const k1 = p.k1_data || {}
              const fa = p.financial_analysis || {}
              return (
                <tr key={p.profile_number} className={p.status === 'error' ? 'batch-row-error' : ''}>
                  <td className="batch-num">{String(p.profile_number).padStart(2, '0')}</td>
                  <td className="batch-name">{p.partnership_name}</td>
                  <td>{p.entity_type || '-'}</td>
                  <td>
                    <span className={`role-badge ${p.is_general_partner ? 'gp' : 'lp'}`}>
                      {p.is_general_partner ? 'GP' : 'LP'}
                    </span>
                  </td>
                  <td className={`batch-amount ${(fa.net_taxable_income || 0) < 0 ? 'amount-negative' : 'amount-positive'}`}>
                    {p.status === 'success' ? formatCurrency(fa.net_taxable_income) : '-'}
                  </td>
                  <td className="batch-amount">
                    {p.status === 'success' ? formatCurrency(k1.capital_account_ending) : '-'}
                  </td>
                  <td className="batch-pii">{p.pii_entities_found ?? '-'}</td>
                  <td>
                    <span className={`status-badge ${p.status}`}>
                      {p.status === 'success' ? 'OK' : 'ERR'}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

const BatchProfileSelector = ({ profiles, selected, onSelect }) => {
  return (
    <div className="batch-selector">
      <label className="batch-selector-label">Select Profile</label>
      <div className="batch-selector-grid">
        {profiles.map((p) => (
          <button
            key={p.profile_number}
            className={`batch-profile-btn ${selected === p.profile_number ? 'active' : ''} ${p.status === 'error' ? 'error' : ''}`}
            onClick={() => onSelect(p.profile_number)}
          >
            <span className="batch-profile-num">{String(p.profile_number).padStart(2, '0')}</span>
            <span className="batch-profile-name">{p.partnership_name}</span>
            <span className="batch-profile-role">
              {p.is_general_partner ? 'GP' : 'LP'} / {p.entity_type || 'Individual'}
            </span>
          </button>
        ))}
      </div>
    </div>
  )
}

const ViewModeTabs = ({ mode, onChangeMode, hasSingle, hasBatch }) => {
  return (
    <div className="view-mode-tabs">
      {hasSingle && (
        <button
          className={`view-tab ${mode === 'single' ? 'active' : ''}`}
          onClick={() => onChangeMode('single')}
        >
          Single K-1 Analysis
        </button>
      )}
      {hasBatch && (
        <button
          className={`view-tab ${mode === 'batch' ? 'active' : ''}`}
          onClick={() => onChangeMode('batch')}
        >
          Batch Processing (10 Profiles)
        </button>
      )}
    </div>
  )
}

/* ================================================================
   App
   ================================================================ */

function App() {
  const [singleData, setSingleData] = useState(null)
  const [batchData, setBatchData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [viewMode, setViewMode] = useState('single')
  const [selectedProfile, setSelectedProfile] = useState(1)

  useEffect(() => {
    Promise.allSettled([
      fetch('/pipeline_results.json').then(r => r.ok ? r.json() : null),
      fetch('/batch/batch_pipeline_results.json').then(r => r.ok ? r.json() : null),
    ]).then(([singleResult, batchResult]) => {
      const single = singleResult.status === 'fulfilled' ? singleResult.value : null
      const batch = batchResult.status === 'fulfilled' ? batchResult.value : null
      setSingleData(single)
      setBatchData(batch)
      if (batch && !single) setViewMode('batch')
      else if (single) setViewMode('single')
      setLoading(false)
    })
  }, [])

  if (loading) {
    return (
      <div className="app">
        <div className="loading">
          <div className="loading-spinner" />
          <p>Loading pipeline results...</p>
        </div>
      </div>
    )
  }

  if (!singleData && !batchData) {
    return (
      <div className="app">
        <div className="error-state">
          <h2>Pipeline Results Not Found</h2>
          <p>
            Run the Dagster pipeline first to generate results. The frontend reads from the pipeline output file.
          </p>
          <code>cd k1_pipeline && uv run dg dev</code>
          <p style={{ fontSize: '0.78rem', marginTop: '0.5rem', color: 'var(--text-muted)' }}>
            Then materialize all assets in the Dagster UI at localhost:3000
          </p>
        </div>
      </div>
    )
  }

  const hasSingle = !!singleData
  const hasBatch = !!batchData

  const batchProfiles = batchData?.profiles || []
  const currentProfile = batchProfiles.find(p => p.profile_number === selectedProfile)
  const profileK1 = currentProfile?.k1_data || {}
  const profileAnalysis = currentProfile?.financial_analysis || {}

  return (
    <div className="app">
      <div className="header">
        <div className="header-left">
          <div className="header-icon">K-1</div>
          <div>
            <h1>K-1 Document Intelligence</h1>
            <p>Automated tax document analysis with PII protection</p>
          </div>
        </div>
        <div className="pipeline-badge">
          <span className="dot" />
          Pipeline Complete
        </div>
      </div>

      <PipelineSteps />

      {(hasSingle && hasBatch) && (
        <ViewModeTabs mode={viewMode} onChangeMode={setViewMode} hasSingle={hasSingle} hasBatch={hasBatch} />
      )}

      {viewMode === 'single' && singleData && (
        <>
          <StatsGrid data={singleData} />
          <div className="main-grid">
            <FinancialDataCard k1={singleData.structured_k1 || singleData.k1_data} />
            <PIICard piiStats={singleData.pii_stats} />
            <PIIComparisonCard comparison={singleData.pii_comparison} />
            <AnalysisCard analysis={singleData.financial_analysis} />
            <ProcessingMeta metadata={singleData.processing_metadata} />
          </div>
        </>
      )}

      {viewMode === 'batch' && batchData && (
        <>
          <BatchOverviewCard batchData={batchData} />
          <BatchProfileSelector
            profiles={batchProfiles}
            selected={selectedProfile}
            onSelect={setSelectedProfile}
          />
          {currentProfile && currentProfile.status === 'success' && (
            <>
              <StatsGrid data={{ k1_data: profileK1, financial_analysis: profileAnalysis, pii_stats: { total_entities_detected: currentProfile.pii_entities_found, entity_counts: {} } }} />
              <div className="main-grid">
                <FinancialDataCard k1={profileK1} />
                <div className="card">
                  <div className="card-header">
                    <h2>Profile Details</h2>
                    <span className={`role-badge ${currentProfile.is_general_partner ? 'gp' : 'lp'}`}>
                      {currentProfile.is_general_partner ? 'General Partner' : 'Limited Partner'}
                    </span>
                  </div>
                  <div className="card-body">
                    <div className="profile-info">
                      <div className="profile-info-row">
                        <span className="profile-info-label">Partnership</span>
                        <span>{currentProfile.partnership_name}</span>
                      </div>
                      <div className="profile-info-row">
                        <span className="profile-info-label">Partner</span>
                        <span>{currentProfile.partner_name}</span>
                      </div>
                      <div className="profile-info-row">
                        <span className="profile-info-label">Entity Type</span>
                        <span>{currentProfile.entity_type}</span>
                      </div>
                      <div className="profile-info-row">
                        <span className="profile-info-label">PII Entities</span>
                        <span>{currentProfile.pii_entities_found} detected</span>
                      </div>
                      <div className="profile-info-row">
                        <span className="profile-info-label">OCR Characters</span>
                        <span>{(currentProfile.ocr_chars || 0).toLocaleString()}</span>
                      </div>
                    </div>
                  </div>
                </div>
                <AnalysisCard analysis={profileAnalysis} />
              </div>
            </>
          )}
          {currentProfile && currentProfile.status === 'error' && (
            <div className="card full-width" style={{ marginTop: '1rem' }}>
              <div className="card-header">
                <h2>Error — Profile {currentProfile.profile_number}</h2>
              </div>
              <div className="card-body">
                <pre style={{ color: 'var(--negative)', fontSize: '0.82rem' }}>{currentProfile.error}</pre>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

export default App
