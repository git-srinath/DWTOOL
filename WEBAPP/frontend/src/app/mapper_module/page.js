'use client'

import React, { useState, useEffect } from 'react'
import { useTheme } from '@/context/ThemeContext'
import ReferenceTable from './ReferenceTable'
import ReferenceForm from './ReferenceForm'

const MapperModule = () => {
  const { darkMode } = useTheme()
  // const muiTheme = useMuiTheme()

  // New state variables for the table view
  const [showReferenceTable, setShowReferenceTable] = useState(true)
  const [showMapperForm, setShowMapperForm] = useState(false)
  
  const [selectedReference, setSelectedReference] = useState(null)

 
  // Function to handle creating a new reference
  const handleCreateNewReference = () => {
    // Reset form data
    setSelectedReference(null)

    // Show the mapper form and hide the reference table
    setShowReferenceTable(false)
    setShowMapperForm(true)
  }

  // Function to handle editing an existing reference
  const handleEditReference = (reference) => {
    if (reference) {
      // The existing fetchReferenceDetails function will be called with this reference
      // fetchReferenceDetails(reference)
      setSelectedReference(reference)
      // Show the mapper form and hide the reference table
      setShowReferenceTable(false)
      setShowMapperForm(true)
    }
  }

  // Function to return to the reference table view
  const handleReturnToReferenceTable = () => {
    setShowMapperForm(false)
    setShowReferenceTable(true)
    // Reset search state
    // setSearchQuery('')
    // Refresh the references list
    // fetchAllReferences()
  }

  return (
    <div
      className={`p-4 min-h-screen ${
        darkMode
          ? 'bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900'
          : 'bg-gradient-to-br from-gray-50 via-white to-gray-50'
      } 
      text-${darkMode ? 'white' : 'gray-800'}`}
      style={{ fontSize: 'clamp(0.875rem, 1vw, 1rem)' }}
    >
      {/* Reference Table View */}
      {showReferenceTable && (
        <ReferenceTable
          handleEditReference={handleEditReference}
          handleCreateNewReference={handleCreateNewReference}
        />
      )}

      {/* Mapper Configuration Form - Updated */}
      {showMapperForm && (
        <ReferenceForm
          handleReturnToReferenceTable={handleReturnToReferenceTable}
          reference={selectedReference}
          l̥
        />
      )}

      <style jsx global>{`
        ::-webkit-scrollbar {
          width: 8px;
          height: 8px;
        }

        ::-webkit-scrollbar-track {
          background: ${darkMode ? '#1F2937' : '#F3F4F6'};
        }

        ::-webkit-scrollbar-thumb {
          background: ${darkMode ? '#4B5563' : '#9CA3AF'};
          border-radius: 4px;
        }

        ::-webkit-scrollbar-thumb:hover {
          background: ${darkMode ? '#6B7280' : '#6B7280'};
        }

        /* Add smooth transitions for all elements */
        * {
          transition: background-color 0.2s ease, border-color 0.2s ease,
            box-shadow 0.2s ease;
        }

        /* Enhance table row hover effect */
        .MuiTableRow-root:hover {
          transition: background-color 0.15s ease !important;
        }

        /* Make buttons more interactive */
        .MuiButton-root,
        .MuiIconButton-root {
          transition: transform 0.1s ease, box-shadow 0.2s ease !important;
        }

        .MuiButton-root:active,
        .MuiIconButton-root:active {
          transform: scale(0.97);
        }

        /* Scaling adjustments */
        .mapper-module-container {
          /* Used to apply scale transformation to the whole component */
          /* transform origin is set to top to avoid layout shift issues */
          margin-bottom: -5vh; /* Compensate for the reduction in height; reduced from -10vh to -5vh */
        }

        /* Adjust typography sizes to be more proportional */
        .MuiTypography-root {
          font-size: 0.9em;
        }

        /* Make form controls more compact */
        .MuiOutlinedInput-root {
          font-size: 0.9rem;
        }

        .MuiInputLabel-root {
          font-size: 0.8rem;
        }

        /* Adjust button sizes for better proportions */
        .MuiButton-root {
          font-size: 0.85rem;
          padding: 4px 16px;
        }

        /* Adjust table cells for better proportions */
        .MuiTableCell-root {
          padding: 8px 12px;
          font-size: 0.85rem;
        }
      `}</style>
    </div>
  )
}

export default MapperModule
