import React, { useState } from 'react';
import './SearchControl.css';

function SearchControl({ onSearch }) {
  const [searchTerm, setSearchTerm] = useState('');
  const [isExpanded, setIsExpanded] = useState(false);

  const locations = [
    { name: 'Skien', lat: 59.2099, lng: 9.6089 },
    { name: 'Porsgrunn', lat: 59.1403, lng: 9.6561 },
    { name: 'Notodden', lat: 59.5606, lng: 9.2594 },
    { name: 'Kragerø', lat: 58.8697, lng: 9.4147 },
    { name: 'Rjukan', lat: 59.8786, lng: 8.5931 },
    { name: 'Bø', lat: 59.4111, lng: 9.0578 },
    { name: 'Nome', lat: 59.5167, lng: 9.1667 },
    { name: 'Drangedal', lat: 59.0833, lng: 9.0667 },
  ];

  const filteredLocations = locations.filter(loc =>
    loc.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleLocationSelect = (location) => {
    onSearch(location);
    setSearchTerm('');
    setIsExpanded(false);
  };

  return (
    <div className="search-control">
      <div className="search-input-wrapper">
        <input
          type="text"
          className="search-input"
          placeholder="Søk etter sted..."
          value={searchTerm}
          onChange={(e) => {
            setSearchTerm(e.target.value);
            setIsExpanded(e.target.value.length > 0);
          }}
          onFocus={() => setIsExpanded(searchTerm.length > 0)}
        />
        <span className="search-icon">🔍</span>
      </div>

      {isExpanded && filteredLocations.length > 0 && (
        <div className="search-results">
          {filteredLocations.map((location, idx) => (
            <div
              key={idx}
              className="search-result-item"
              onClick={() => handleLocationSelect(location)}
            >
              📍 {location.name}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default SearchControl;
