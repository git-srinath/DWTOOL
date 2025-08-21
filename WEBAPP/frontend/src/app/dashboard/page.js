"use client"

import React from 'react';
import { Box, Typography, Paper } from '@mui/material';

const DashboardPage = () => {
  const metabaseDashboardUrl = "http://172.16.20.69:3002/public/dashboard/c13c3045-50ad-428b-a06b-0634f23204f9?tab=5-tab1";
  
  return (
    <Box sx={{ width: '100%', height: '100%', p: 2 }}>

      
      <Paper 
        elevation={0} 
        sx={{ 
          mt: 2, 
          height: 'calc(100vh - 180px)', 
          width: '100%',
          overflow: 'hidden',
          border: '1px solid rgba(0, 0, 0, 0.12)',
          borderRadius: 1
        }}
      >
        <iframe
          src={metabaseDashboardUrl}
          width="100%"
          height="100%"
          frameBorder="0"
          allowTransparency="true"
          style={{ border: 'none' }}
          title="Metabase Dashboard"
        />
      </Paper>
    </Box>
  );
};

export default DashboardPage;
