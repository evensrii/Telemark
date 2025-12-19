import React, { useState } from 'react';
import './LayerControl.css';

function LayerControl({ layers, activeLayers, onLayerToggle }) {
  const [isExpanded, setIsExpanded] = useState(true);

  return (
    <div className={`layer-control ${isExpanded ? 'expanded' : 'collapsed'}`}>
      <div className="layer-control-header" onClick={() => setIsExpanded(!isExpanded)}>
        <h3>Kartlag</h3>
        <button className="toggle-btn">
          {isExpanded ? '−' : '+'}
        </button>
      </div>
      
      {isExpanded && (
        <div className="layer-control-content">
          <p className="layer-control-description">
            Velg hvilke lag som skal vises på kartet
          </p>
          
          <div className="layer-list">
            {layers.map(layer => (
              <div key={layer.id} className="layer-item">
                <label className="layer-checkbox">
                  <input
                    type="checkbox"
                    checked={activeLayers[layer.id]}
                    onChange={() => onLayerToggle(layer.id)}
                  />
                  <span className="checkmark"></span>
                  <span className="layer-name">{layer.name}</span>
                </label>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default LayerControl;
