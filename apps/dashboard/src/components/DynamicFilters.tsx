import React from 'react'

interface DynamicFiltersProps {
  availableTypes: string[]
  filters: { [key: string]: boolean }
  onFilterChange: (type: string, enabled: boolean) => void
}

const DynamicFilters: React.FC<DynamicFiltersProps> = ({ 
  availableTypes, 
  filters, 
  onFilterChange 
}) => {
  const getSeverityColor = (type: string) => {
    const colors: { [key: string]: string } = {
      military: '#ff6b6b',
      political: '#4ecdc4',
      infrastructure: '#45b7d1',
      economic: '#f9ca24',
      general: '#6c5ce7',
      security: '#fd79a8',
      diplomatic: '#a29bfe',
      environmental: '#00b894'
    }
    return colors[type] || '#95a5a6'
  }

  const getEventCount = (_type: string) => {
    // This would be calculated from the actual events
    // For now, return a placeholder
    return Math.floor(Math.random() * 20) + 1
  }

  const handleToggleAll = (enabled: boolean) => {
    availableTypes.forEach(type => {
      onFilterChange(type, enabled)
    })
  }

  if (availableTypes.length === 0) {
    return (
      <div style={{
        backgroundColor: '#1a1a1a',
        padding: '15px',
        margin: '10px',
        borderRadius: '8px',
        color: '#fff'
      }}>
        <h3 style={{ margin: '0 0 10px 0', fontSize: '14px' }}>Event Types</h3>
        <p style={{ margin: 0, fontSize: '12px', color: '#999' }}>
          No event types available yet
        </p>
      </div>
    )
  }

  return (
    <div style={{
      backgroundColor: '#1a1a1a',
      padding: '15px',
      margin: '10px',
      borderRadius: '8px',
      color: '#fff'
    }}>
      <div style={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center',
        marginBottom: '15px'
      }}>
        <h3 style={{ margin: 0, fontSize: '14px' }}>
          Event Types ({availableTypes.length})
        </h3>
        <div style={{ fontSize: '12px' }}>
          <button
            onClick={() => handleToggleAll(true)}
            style={{
              backgroundColor: '#27ae60',
              color: '#fff',
              border: 'none',
              padding: '2px 6px',
              borderRadius: '3px',
              marginRight: '5px',
              cursor: 'pointer'
            }}
          >
            All
          </button>
          <button
            onClick={() => handleToggleAll(false)}
            style={{
              backgroundColor: '#e74c3c',
              color: '#fff',
              border: 'none',
              padding: '2px 6px',
              borderRadius: '3px',
              cursor: 'pointer'
            }}
          >
            None
          </button>
        </div>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
        {availableTypes.map(type => (
          <div
            key={type}
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              padding: '8px',
              backgroundColor: filters[type] ? '#2a2a2a' : '#1a1a1a',
              borderRadius: '4px',
              border: `1px solid ${getSeverityColor(type)}`,
              cursor: 'pointer',
              transition: 'all 0.2s ease'
            }}
            onClick={() => onFilterChange(type, !filters[type])}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <div
                style={{
                  width: '12px',
                  height: '12px',
                  borderRadius: '50%',
                  backgroundColor: getSeverityColor(type),
                  opacity: filters[type] ? 1 : 0.3
                }}
              />
              <span style={{ 
                fontSize: '12px',
                opacity: filters[type] ? 1 : 0.5,
                textTransform: 'capitalize'
              }}>
                {type}
              </span>
            </div>
            
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ 
                fontSize: '10px', 
                color: '#999',
                backgroundColor: '#333',
                padding: '2px 6px',
                borderRadius: '3px'
              }}>
                {getEventCount(type)}
              </span>
              
              <div style={{
                width: '16px',
                height: '16px',
                border: '2px solid #555',
                borderRadius: '3px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                backgroundColor: filters[type] ? getSeverityColor(type) : 'transparent'
              }}>
                {filters[type] && (
                  <span style={{ fontSize: '10px', color: '#fff' }}>✓</span>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>

      <div style={{
        marginTop: '15px',
        paddingTop: '10px',
        borderTop: '1px solid #333',
        fontSize: '11px',
        color: '#666'
      }}>
        <div style={{ marginBottom: '5px' }}>
          Active: {Object.values(filters).filter(Boolean).length}/{availableTypes.length}
        </div>
        <div>
          Total Events: {availableTypes.reduce((sum, type) => sum + getEventCount(type), 0)}
        </div>
      </div>
    </div>
  )
}

export default DynamicFilters
