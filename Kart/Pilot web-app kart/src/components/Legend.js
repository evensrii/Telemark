import React, { useState } from 'react';
import './Legend.css';

function Legend({ legends }) {
  const [isExpanded, setIsExpanded] = useState(true);

  return (
    <div className={`legend ${isExpanded ? 'expanded' : 'collapsed'}`}>
      <div className="legend-header" onClick={() => setIsExpanded(!isExpanded)}>
        <h3>Tegnforklaring</h3>
        <button className="toggle-btn">
          {isExpanded ? '−' : '+'}
        </button>
      </div>
      
      {isExpanded && (
        <div className="legend-content">
          {legends.map((legend, idx) => (
            <div key={idx} className="legend-section">
              <h4>{legend.title}</h4>
              <div className="legend-items">
                {legend.items.map((item, itemIdx) => (
                  <div key={itemIdx} className="legend-item">
                    <span 
                      className="legend-color" 
                      style={{ backgroundColor: item.color }}
                    ></span>
                    <span className="legend-label">{item.label}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default Legend;
