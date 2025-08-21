import React from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  Box,
  IconButton,
  Tooltip,
  Chip,
  Badge,
  useMediaQuery
} from '@mui/material';
import { styled, alpha, useTheme as useMuiTheme } from '@mui/material/styles';
import { 
  Error as ErrorIcon, 
  Info as InfoIcon,
  CheckCircle as CheckCircleIcon,
  TrendingUp as TrendingUpIcon, 
  TrendingDown as TrendingDownIcon
} from '@mui/icons-material';
import { format } from 'date-fns';
import { motion } from 'framer-motion';

// Styled components
const StyledTableContainer = styled(TableContainer)(({ theme, darkMode }) => ({
  borderRadius: theme.shape.borderRadius,
  overflow: 'auto',
  maxHeight: '60vh',
  '&::-webkit-scrollbar': {
    width: '8px',
    height: '8px',
  },
  '&::-webkit-scrollbar-track': {
    backgroundColor: darkMode ? alpha('#FFFFFF', 0.05) : alpha('#000000', 0.05),
    borderRadius: '8px',
  },
  '&::-webkit-scrollbar-thumb': {
    backgroundColor: darkMode ? alpha('#FFFFFF', 0.2) : alpha('#000000', 0.2),
    borderRadius: '8px',
    '&:hover': {
      backgroundColor: darkMode ? alpha('#FFFFFF', 0.3) : alpha('#000000', 0.3),
    }
  },
}));

const StyledTable = styled(Table)(({ theme, darkMode }) => ({
  '& .MuiTableCell-head': {
    backgroundColor: darkMode ? alpha(theme.palette.primary.dark, 0.3) : alpha(theme.palette.primary.light, 0.1),
    color: darkMode ? theme.palette.primary.light : theme.palette.primary.dark,
    fontWeight: 600,
    fontSize: '0.8rem',
    padding: '12px 16px',
    whiteSpace: 'nowrap',
    position: 'sticky',
    top: 0,
    zIndex: 10,
  }
}));

const StyledTableRow = styled(TableRow)(({ theme, darkMode, status }) => {
  let bgColor = 'transparent';
  let hoverColor = darkMode ? alpha('#FFFFFF', 0.05) : alpha('#000000', 0.03);
  
  // Status-based coloring
  if (status === 'FL') {
    bgColor = darkMode ? alpha(theme.palette.error.dark, 0.1) : alpha(theme.palette.error.light, 0.05);
    hoverColor = darkMode ? alpha(theme.palette.error.dark, 0.15) : alpha(theme.palette.error.light, 0.1);
  } else if (status === 'SC') {
    bgColor = darkMode ? alpha(theme.palette.success.dark, 0.05) : alpha(theme.palette.success.light, 0.03);
    hoverColor = darkMode ? alpha(theme.palette.success.dark, 0.1) : alpha(theme.palette.success.light, 0.07);
  } else if (status === 'RN') {
    bgColor = darkMode ? alpha(theme.palette.info.dark, 0.05) : alpha(theme.palette.info.light, 0.03);
    hoverColor = darkMode ? alpha(theme.palette.info.dark, 0.1) : alpha(theme.palette.info.light, 0.07);
  }
  
  return {
    backgroundColor: bgColor,
    transition: 'background-color 0.2s ease',
    '&:hover': {
      backgroundColor: hoverColor,
    },
    '& > .MuiTableCell-root': {
      borderBottom: darkMode 
        ? `1px solid ${alpha('#FFFFFF', 0.1)}` 
        : `1px solid ${alpha('#000000', 0.1)}`,
      padding: '10px 16px',
    },
  };
});

const ActionButton = styled(IconButton)(({ theme, darkMode }) => ({
  backgroundColor: darkMode ? alpha(theme.palette.error.main, 0.1) : alpha(theme.palette.error.main, 0.05),
  '&:hover': {
    backgroundColor: darkMode ? alpha(theme.palette.error.main, 0.2) : alpha(theme.palette.error.main, 0.1),
  },
  marginRight: theme.spacing(0.75),
  padding: 6,
  transition: 'all 0.2s ease',
}));

const LogStatusChip = styled(Chip)(({ theme, status, darkMode }) => {
  let color;
  let backgroundColor;
  let borderColor;
  
  if (status === 'SC') {
    color = darkMode ? theme.palette.success.light : theme.palette.success.dark;
    backgroundColor = darkMode ? alpha(theme.palette.success.main, 0.2) : alpha(theme.palette.success.main, 0.1);
    borderColor = darkMode ? alpha(theme.palette.success.main, 0.3) : alpha(theme.palette.success.main, 0.2);
  } else if (status === 'FL') {
    color = darkMode ? theme.palette.error.light : theme.palette.error.dark;
    backgroundColor = darkMode ? alpha(theme.palette.error.main, 0.2) : alpha(theme.palette.error.main, 0.1);
    borderColor = darkMode ? alpha(theme.palette.error.main, 0.3) : alpha(theme.palette.error.main, 0.2);
  } else if (status === 'RN') {
    color = darkMode ? theme.palette.info.light : theme.palette.info.dark;
    backgroundColor = darkMode ? alpha(theme.palette.info.main, 0.2) : alpha(theme.palette.info.main, 0.1);
    borderColor = darkMode ? alpha(theme.palette.info.main, 0.3) : alpha(theme.palette.info.main, 0.2);
  } else {
    color = darkMode ? theme.palette.grey[300] : theme.palette.grey[700];
    backgroundColor = darkMode ? alpha(theme.palette.grey[500], 0.2) : alpha(theme.palette.grey[500], 0.1);
    borderColor = darkMode ? alpha(theme.palette.grey[500], 0.3) : alpha(theme.palette.grey[500], 0.2);
  }
  
  return {
    color: color,
    backgroundColor: backgroundColor,
    fontWeight: 500,
    fontSize: '0.75rem',
    borderRadius: '16px',
    border: `1px solid ${borderColor}`,
    '& .MuiChip-icon': {
      color: 'inherit',
      marginRight: '4px',
    },
    transition: 'all 0.2s ease',
  };
});

const ValueWithTrend = styled(Box)(({ theme, value, comparison = 0, darkMode }) => {
  const change = value - comparison;
  const hasChange = comparison > 0 && change !== 0;
  
  let color = 'inherit';
  if (hasChange) {
    color = change > 0 
      ? (darkMode ? theme.palette.success.light : theme.palette.success.main)
      : (darkMode ? theme.palette.error.light : theme.palette.error.main);
  }
  
  return {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-start',
    color: color,
    fontWeight: hasChange ? 500 : 400,
    gap: '4px',
    transition: 'all 0.2s ease',
  };
});

const ErrorBadge = styled(Badge)(({ theme }) => ({
  '& .MuiBadge-badge': {
    backgroundColor: theme.palette.error.main,
    color: theme.palette.error.contrastText,
    fontWeight: 'bold',
    fontSize: '0.65rem',
    height: '18px',
    minWidth: '18px',
  }
}));

// Function to get log status label
const getLogStatusLabel = (status) => {
  if (!status) return 'Unknown';
  if (status === 'SC') return 'Success';
  if (status === 'FL') return 'Failed';
  if (status === 'RN') return 'Running';
  return status;
};

// Function to get status icon
const getStatusIcon = (status) => {
  if (!status) return <InfoIcon fontSize="small" />;
  if (status === 'SC') return <CheckCircleIcon fontSize="small" />;
  if (status === 'FL') return <ErrorIcon fontSize="small" />;
  if (status === 'RN') return <InfoIcon fontSize="small" />;
  return null;
};

// Format date safely
const formatDate = (dateStr, formatString = 'yyyy-MM-dd') => {
  if (!dateStr) return '-';
  try {
    return format(new Date(dateStr), formatString);
  } catch (error) {
    console.error(`Error formatting date: ${dateStr}`, error);
    return dateStr || '-';
  }
};

const LogDetailsTable = ({ logDetails, darkMode, onViewErrors }) => {
  const muiTheme = useMuiTheme();
  const isMobile = useMediaQuery(muiTheme.breakpoints.down('md'));
  
  // Make sure logDetails is an array
  const logs = Array.isArray(logDetails) ? logDetails : [];
  
  // Filter out any undefined or badly formed records
  const validLogs = logs.filter(log => log && typeof log === 'object');
  
  return (
    <StyledTableContainer darkMode={darkMode}>
      <StyledTable stickyHeader darkMode={darkMode} size={isMobile ? "small" : "medium"}>
        <TableHead>
          <TableRow>
            <TableCell>Process Date</TableCell>
            <TableCell>Job ID</TableCell>
            <TableCell align="right">Source Rows</TableCell>
            <TableCell align="right">Target Rows</TableCell>
            <TableCell align="right">Error Rows</TableCell>
            <TableCell>Start Time</TableCell>
            <TableCell>End Time</TableCell>
            <TableCell>Status</TableCell>
            <TableCell align="center">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {validLogs.map((log, index) => {
            // Find previous log for comparison if available
            const prevLog = index < validLogs.length - 1 ? validLogs[index + 1] : null;
            
            return (
              <StyledTableRow 
                key={index}
                component={motion.tr}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.3, delay: index * 0.05 }}
                darkMode={darkMode}
                status={log.STATUS}
              >
                <TableCell>
                  {formatDate(log.PROCESS_DATE, 'yyyy-MM-dd')}
                </TableCell>
                <TableCell>{log.JOB_ID || '-'}</TableCell>
                <TableCell align="right">
                  <ValueWithTrend 
                    darkMode={darkMode} 
                    value={isNaN(log.SOURCE_ROWS) ? 0 : log.SOURCE_ROWS || 0}
                    comparison={prevLog ? (isNaN(prevLog.SOURCE_ROWS) ? 0 : prevLog.SOURCE_ROWS || 0) : 0}
                  >
                    {isNaN(log.SOURCE_ROWS) ? 0 : log.SOURCE_ROWS || 0}
                    {prevLog && log.SOURCE_ROWS > prevLog.SOURCE_ROWS && (
                      <TrendingUpIcon fontSize="small" />
                    )}
                    {prevLog && log.SOURCE_ROWS < prevLog.SOURCE_ROWS && (
                      <TrendingDownIcon fontSize="small" />
                    )}
                  </ValueWithTrend>
                </TableCell>
                <TableCell align="right">
                  <ValueWithTrend 
                    darkMode={darkMode} 
                    value={isNaN(log.TARGET_ROWS) ? 0 : log.TARGET_ROWS || 0}
                    comparison={prevLog ? (isNaN(prevLog.TARGET_ROWS) ? 0 : prevLog.TARGET_ROWS || 0) : 0}
                  >
                    {isNaN(log.TARGET_ROWS) ? 0 : log.TARGET_ROWS || 0}
                    {prevLog && log.TARGET_ROWS > prevLog.TARGET_ROWS && (
                      <TrendingUpIcon fontSize="small" />
                    )}
                    {prevLog && log.TARGET_ROWS < prevLog.TARGET_ROWS && (
                      <TrendingDownIcon fontSize="small" />
                    )}
                  </ValueWithTrend>
                </TableCell>
                <TableCell align="right">
                  {(log.ERROR_ROWS && log.ERROR_ROWS > 0) ? (
                    <ErrorBadge badgeContent={isNaN(log.ERROR_ROWS) ? 0 : log.ERROR_ROWS || 0} color="error" overlap="rectangular">
                      <Typography 
                        variant="body2" 
                        color="error.main"
                        fontWeight={600}
                        component="span"
                      >
                        {isNaN(log.ERROR_ROWS) ? 0 : log.ERROR_ROWS || 0}
                      </Typography>
                    </ErrorBadge>
                  ) : (
                    <Typography variant="body2" component="span">
                      0
                    </Typography>
                  )}
                </TableCell>
                <TableCell>
                  {formatDate(log.START_DATE, 'yyyy-MM-dd HH:mm:ss')}
                </TableCell>
                <TableCell>
                  {formatDate(log.END_DATE, 'yyyy-MM-dd HH:mm:ss')}
                </TableCell>
                <TableCell>
                  <LogStatusChip
                    status={log.STATUS}
                    darkMode={darkMode}
                    label={getLogStatusLabel(log.STATUS)}
                    size="small"
                    icon={getStatusIcon(log.STATUS)}
                  />
                </TableCell>
                <TableCell align="center">
                  {log.STATUS === 'FL' && log.JOB_ID && (
                    <Tooltip title="View Errors">
                      <ActionButton
                        darkMode={darkMode}
                        onClick={() => onViewErrors(log.JOB_ID)}
                        size="small"
                      >
                        <ErrorIcon fontSize="small" />
                      </ActionButton>
                    </Tooltip>
                  )}
                </TableCell>
              </StyledTableRow>
            );
          })}
        </TableBody>
      </StyledTable>
    </StyledTableContainer>
  );
};

export default LogDetailsTable; 