import React from 'react';
import './CompanyInfoBox.css';

function CompanyInfoBox({ 
  companyCount, 
  totalEmployees, 
  filterMinEmployees, 
  onFilterChange,
  isVisible 
}) {
  if (!isVisible) return null;

  return (
    <div className="company-info-box">
      <div className="info-box-header">
        <h4>Bedriftsstatistikk</h4>
      </div>
      
      <div className="info-box-content">
        <div className="filter-section">
          <label className="filter-checkbox">
            <input
              type="checkbox"
              checked={filterMinEmployees}
              onChange={(e) => onFilterChange(e.target.checked)}
            />
            <span className="checkmark"></span>
            <span className="filter-label">Vis kun bedrifter med 5+ ansatte</span>
          </label>
        </div>

        <div className="stats-section">
          <div className="stat-item">
            <span className="stat-label">Antall bedrifter:</span>
            <span className="stat-value">{companyCount.toLocaleString('nb-NO')}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">Totalt antall ansatte:</span>
            <span className="stat-value">{totalEmployees.toLocaleString('nb-NO')}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default CompanyInfoBox;
