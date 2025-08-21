import React from 'react';
import { Box, Tab, Tabs, useTheme, alpha } from '@mui/material';
import { motion } from 'framer-motion';
import AssignmentIcon from '@mui/icons-material/Assignment';
import SecurityIcon from '@mui/icons-material/Security';
import HistoryIcon from '@mui/icons-material/History';
import VpnKeyIcon from '@mui/icons-material/VpnKey';
import InfoIcon from '@mui/icons-material/Info';

const TabsLayout = ({ activeTab, handleTabChange, isMobile }) => {
  const theme = useTheme();

  return (
    <Box sx={{ 
      width: '100%', 
      mb: 1,
      position: 'sticky',
      top: 0,
      zIndex: 10,
      backdropFilter: 'blur(10px)',
      background: alpha(theme.palette.background.default, 0.8),
      borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
    }}>
      <Tabs
        value={activeTab}
        onChange={handleTabChange}
        variant={isMobile ? 'scrollable' : 'standard'}
        scrollButtons={isMobile ? 'auto' : false}
        sx={{
          minHeight: 40,
          '& .MuiTabs-indicator': {
            height: 2,
            borderTopLeftRadius: 2,
            borderTopRightRadius: 2,
            background: `linear-gradient(90deg, ${theme.palette.primary.main}, ${alpha(theme.palette.primary.light, 0.8)})`,
            boxShadow: `0 0 6px ${alpha(theme.palette.primary.main, 0.5)}`,
          },
          '& .MuiTabs-flexContainer': {
            gap: 0.5,
          },
          '& .MuiTab-root': {
            py: 0.5,
            px: 1.5,
            minHeight: 40,
            textTransform: 'none',
            color: theme.palette.text.secondary,
            borderRadius: '8px 8px 0 0',
            transition: 'all 0.3s ease',
            opacity: 0.7,
            fontSize: '0.85rem',
            '&::after': {
              content: '""',
              position: 'absolute',
              bottom: 0,
              left: '20%',
              right: '20%',
              height: '2px',
              backgroundColor: theme.palette.primary.main,
              transform: 'scaleX(0)',
              transition: 'transform 0.3s ease',
            },
            '&.Mui-selected': {
              color: theme.palette.primary.main,
              fontWeight: 600,
              opacity: 1,
              backgroundColor: alpha(theme.palette.primary.main, 0.05),
            },
            '&:hover': {
              color: theme.palette.primary.main,
              opacity: 1,
              backgroundColor: alpha(theme.palette.primary.main, 0.03),
              '&::after': {
                transform: 'scaleX(1)',
              },
              '&.Mui-selected::after': {
                transform: 'scaleX(0)',
              }
            }
          },
        }}
      >
        <Tab
          icon={
            <motion.div
              whileHover={{ scale: 1.1, rotate: 5 }}
              whileTap={{ scale: 0.9 }}
            >
              <AssignmentIcon sx={{ fontSize: '1rem', mr: 0.5 }} />
            </motion.div>
          }
          iconPosition="start"
          label="Users"
        />
        <Tab
          icon={
            <motion.div
              whileHover={{ scale: 1.1, rotate: 5 }}
              whileTap={{ scale: 0.9 }}
            >
              <SecurityIcon sx={{ fontSize: '1rem', mr: 0.5 }} />
            </motion.div>
          }
          iconPosition="start"
          label="Roles"
        />
        <Tab
          icon={
            <motion.div
              whileHover={{ scale: 1.1, rotate: 5 }}
              whileTap={{ scale: 0.9 }}
            >
              <HistoryIcon sx={{ fontSize: '1rem', mr: 0.5 }} />
            </motion.div>
          }
          iconPosition="start"
          label="Audit Logs"
        />
        <Tab
          icon={
            <motion.div
              whileHover={{ scale: 1.1, rotate: 5 }}
              whileTap={{ scale: 0.9 }}
            >
              <VpnKeyIcon sx={{ fontSize: '1rem', mr: 0.5 }} />
            </motion.div>
          }
          iconPosition="start"
          label="License"
        />
        <Tab
          icon={
            <motion.div
              whileHover={{ scale: 1.1, rotate: 5 }}
              whileTap={{ scale: 0.9 }}
            >
              <InfoIcon sx={{ fontSize: '1rem', mr: 0.5 }} />
            </motion.div>
          }
          iconPosition="start"
          label="About"
        />
      </Tabs>
    </Box>
  );
};

export default TabsLayout; 