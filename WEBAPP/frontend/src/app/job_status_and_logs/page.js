"use client";

import React, { useState, useMemo } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  Table, 
  TableBody, 
  TableCell, 
  TableContainer, 
  TableHead, 
  TableRow,
  Tooltip,
  IconButton,
  Alert,
  Chip,
  Skeleton,
  Stack,
  Divider,
  Badge,
  LinearProgress,
  useMediaQuery,
  TextField,
  InputAdornment,
  Button,
  Menu,
  MenuItem,
  Checkbox,
  ListItemText,
  FormControl,
  InputLabel,
  Select,
  OutlinedInput
} from '@mui/material';
import { styled, alpha, useTheme as useMuiTheme } from '@mui/material/styles';
import { 
  Visibility as VisibilityIcon,
  Info as InfoIcon,
  Error as ErrorIcon,
  Assignment as AssignmentIcon,
  Refresh as RefreshIcon,
  Schedule as ScheduleIcon,
  AccessTime as AccessTimeIcon,
  CalendarMonth as CalendarMonthIcon,
  Today as TodayIcon,
  Event as EventIcon,
  Update as UpdateIcon,
  Search as SearchIcon,
  FilterAlt as FilterAltIcon,
  Clear as ClearIcon
} from '@mui/icons-material';
import { motion, AnimatePresence } from 'framer-motion';
import { useTheme } from '@/context/ThemeContext';
import useJobLogs from '@/hooks/useJobLogs';
import LogDetailsDialog from './LogDetailsDialog';
import ErrorDetailsDialog from './ErrorDetailsDialog';

// Styled components
const PageHeader = styled(Box)(({ theme, darkMode }) => ({
  marginBottom: theme.spacing(4),
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(1),
}));

const HeaderTitle = styled(Typography)(({ theme }) => ({
  fontSize: '1.75rem',
  fontWeight: 700,
  lineHeight: 1.2,
  [theme.breakpoints.up('md')]: {
    fontSize: '2rem',
  }
}));

const HeaderSubtitle = styled(Typography)(({ theme, darkMode }) => ({
  fontSize: '0.875rem',
  color: darkMode ? alpha(theme.palette.common.white, 0.7) : alpha(theme.palette.common.black, 0.6),
  maxWidth: '800px',
}));

const StyledTableContainer = styled(TableContainer)(({ theme, darkMode }) => ({
  maxHeight: '70vh',
  borderRadius: theme.shape.borderRadius,
  '& .MuiTableCell-head': {
    backgroundColor: darkMode ? '#1A202C' : '#F7FAFC', 
    color: darkMode ? '#E2E8F0' : '#2D3748',
    fontWeight: 600,
    fontSize: '0.875rem',
    padding: '12px 16px',
    whiteSpace: 'nowrap',
    borderBottom: darkMode ? '1px solid rgba(255, 255, 255, 0.1)' : '1px solid rgba(0, 0, 0, 0.1)'
  },
  '& .MuiTableCell-body': {
    color: darkMode ? '#E2E8F0' : '#2D3748',
    padding: '8px 16px',
    fontSize: '0.875rem',
  },
  '& .MuiTableRow-root:hover': {
    backgroundColor: darkMode ? 'rgba(255, 255, 255, 0.05)' : 'rgba(0, 0, 0, 0.02)',
    transition: 'background-color 0.2s ease',
  },
}));

const ActionButton = styled(IconButton)(({ theme, darkMode }) => ({
  backgroundColor: darkMode ? 'rgba(66, 153, 225, 0.1)' : 'rgba(66, 153, 225, 0.05)',
  '&:hover': {
    backgroundColor: darkMode ? 'rgba(66, 153, 225, 0.2)' : 'rgba(66, 153, 225, 0.1)',
  },
  marginRight: theme.spacing(0.75),
  padding: 6,
  transition: 'all 0.2s ease',
}));

const StatusChip = styled(Chip)(({ theme, status, darkMode }) => {
  let color;
  let backgroundColor;
  
  if (status === 'A') {
    color = darkMode ? '#C6F6D5' : '#22543D';
    backgroundColor = darkMode ? 'rgba(72, 187, 120, 0.2)' : 'rgba(72, 187, 120, 0.1)';
  } else if (status === 'I') {
    color = darkMode ? '#FED7D7' : '#822727';
    backgroundColor = darkMode ? 'rgba(245, 101, 101, 0.2)' : 'rgba(245, 101, 101, 0.1)';
  } else {
    color = darkMode ? '#EDF2F7' : '#4A5568';
    backgroundColor = darkMode ? 'rgba(160, 174, 192, 0.2)' : 'rgba(160, 174, 192, 0.1)';
  }
  
  return {
    color: color,
    backgroundColor: backgroundColor,
    fontWeight: 500,
    fontSize: '0.75rem',
    borderRadius: '4px',
    '& .MuiChip-icon': {
      color: 'inherit',
    },
    transition: 'all 0.2s ease',
    border: `1px solid ${darkMode ? 'transparent' : alpha(color, 0.3)}`,
  };
});

const CardHeader = styled(Box)(({ theme, darkMode }) => ({
  padding: theme.spacing(2),
  borderBottom: darkMode ? '1px solid rgba(255, 255, 255, 0.1)' : '1px solid rgba(0, 0, 0, 0.1)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
}));

const RefreshIconButton = styled(IconButton)(({ theme, darkMode }) => ({
  color: darkMode ? theme.palette.primary.light : theme.palette.primary.main,
  backgroundColor: darkMode ? alpha(theme.palette.primary.main, 0.1) : alpha(theme.palette.primary.main, 0.05),
  '&:hover': {
    backgroundColor: darkMode ? alpha(theme.palette.primary.main, 0.2) : alpha(theme.palette.primary.main, 0.1),
  },
  transition: 'all 0.2s ease',
}));

const ScheduleChip = styled(Box)(({ theme, darkMode, frequencyType }) => {
  // Define colors based on frequency type
  let bgColor, borderColor, textColor;

  switch (frequencyType) {
    case 'DLY':
      bgColor = darkMode ? alpha(theme.palette.success.main, 0.1) : alpha(theme.palette.success.main, 0.05);
      borderColor = darkMode ? alpha(theme.palette.success.main, 0.3) : alpha(theme.palette.success.main, 0.2);
      textColor = darkMode ? theme.palette.success.light : theme.palette.success.dark;
      break;
    case 'WKL':
      bgColor = darkMode ? alpha(theme.palette.primary.main, 0.1) : alpha(theme.palette.primary.main, 0.05);
      borderColor = darkMode ? alpha(theme.palette.primary.main, 0.3) : alpha(theme.palette.primary.main, 0.2);
      textColor = darkMode ? theme.palette.primary.light : theme.palette.primary.dark;
      break;
    case 'MTH':
      bgColor = darkMode ? alpha(theme.palette.secondary.main, 0.1) : alpha(theme.palette.secondary.main, 0.05);
      borderColor = darkMode ? alpha(theme.palette.secondary.main, 0.3) : alpha(theme.palette.secondary.main, 0.2);
      textColor = darkMode ? theme.palette.secondary.light : theme.palette.secondary.dark;
      break;
    case 'ADH':
      bgColor = darkMode ? alpha(theme.palette.warning.main, 0.1) : alpha(theme.palette.warning.main, 0.05);
      borderColor = darkMode ? alpha(theme.palette.warning.main, 0.3) : alpha(theme.palette.warning.main, 0.2);
      textColor = darkMode ? theme.palette.warning.light : theme.palette.warning.dark;
      break;
    default:
      bgColor = darkMode ? alpha(theme.palette.info.main, 0.1) : alpha(theme.palette.info.main, 0.05);
      borderColor = darkMode ? alpha(theme.palette.info.main, 0.3) : alpha(theme.palette.info.main, 0.2);
      textColor = darkMode ? theme.palette.info.light : theme.palette.info.dark;
  }

  return {
    display: 'flex',
    alignItems: 'center',
    padding: '6px 10px',
    borderRadius: '16px',
    backgroundColor: bgColor,
    border: `1px solid ${borderColor}`,
    color: textColor,
    fontSize: '0.8rem',
    width: 'fit-content',
    gap: '6px',
    transition: 'all 0.2s ease',
    '&:hover': {
      backgroundColor: alpha(bgColor, 1.5),
    }
  };
});

// Styled search components
const SearchBar = styled(TextField)(({ theme, darkMode }) => ({
  '.MuiOutlinedInput-root': {
    borderRadius: '20px',
    backgroundColor: darkMode ? alpha(theme.palette.common.white, 0.05) : alpha(theme.palette.common.black, 0.02),
    transition: 'all 0.2s ease',
    '&:hover': {
      backgroundColor: darkMode ? alpha(theme.palette.common.white, 0.08) : alpha(theme.palette.common.black, 0.04),
    },
    '&.Mui-focused': {
      backgroundColor: darkMode ? alpha(theme.palette.common.white, 0.1) : alpha(theme.palette.common.white, 1),
      boxShadow: `0 0 0 2px ${darkMode ? alpha(theme.palette.primary.main, 0.2) : alpha(theme.palette.primary.main, 0.1)}`
    }
  }
}));

const FilterButton = styled(Button)(({ theme, darkMode, active }) => ({
  borderRadius: '20px',
  textTransform: 'none',
  backgroundColor: active ? 
    (darkMode ? alpha(theme.palette.primary.main, 0.2) : alpha(theme.palette.primary.main, 0.1)) : 
    'transparent',
  border: `1px solid ${active ? 
    (darkMode ? theme.palette.primary.main : theme.palette.primary.main) : 
    (darkMode ? alpha(theme.palette.common.white, 0.1) : alpha(theme.palette.common.black, 0.1))}`,
  color: active ? 
    (darkMode ? theme.palette.primary.light : theme.palette.primary.main) : 
    (darkMode ? theme.palette.common.white : theme.palette.common.black),
  padding: '4px 12px',
  fontSize: '0.875rem',
  fontWeight: active ? 500 : 400,
  '&:hover': {
    backgroundColor: active ? 
      (darkMode ? alpha(theme.palette.primary.main, 0.3) : alpha(theme.palette.primary.main, 0.15)) : 
      (darkMode ? alpha(theme.palette.common.white, 0.05) : alpha(theme.palette.common.black, 0.05))
  }
}));

const FilterChip = styled(Chip)(({ theme, darkMode }) => ({
  margin: theme.spacing(0.5),
  borderRadius: '16px',
  backgroundColor: darkMode ? alpha(theme.palette.primary.main, 0.15) : alpha(theme.palette.primary.main, 0.1),
  color: darkMode ? theme.palette.primary.light : theme.palette.primary.main,
  border: `1px solid ${darkMode ? alpha(theme.palette.primary.main, 0.3) : alpha(theme.palette.primary.main, 0.2)}`,
  fontWeight: 500,
  '& .MuiChip-deleteIcon': {
    color: darkMode ? alpha(theme.palette.primary.light, 0.7) : alpha(theme.palette.primary.main, 0.7),
    '&:hover': {
      color: darkMode ? theme.palette.primary.light : theme.palette.primary.main,
    }
  }
}));

const JobStatusAndLogsPage = () => {
  const { darkMode } = useTheme();
  const muiTheme = useMuiTheme();
  const isMobile = useMediaQuery(muiTheme.breakpoints.down('sm'));
  
  const {
    scheduledJobs,
    logDetails,
    errorDetails,
    loading,
    logLoading,
    errorLoading,
    error,
    fetchScheduledJobs,
    fetchJobLogs,
    fetchErrorDetails
  } = useJobLogs();
  
  const [selectedMapRef, setSelectedMapRef] = useState(null);
  const [selectedJobId, setSelectedJobId] = useState(null);
  const [openLogDialog, setOpenLogDialog] = useState(false);
  const [openErrorDialog, setOpenErrorDialog] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  
  // Search and filter states
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState([]);
  const [frequencyFilter, setFrequencyFilter] = useState([]);
  const [filterAnchorEl, setFilterAnchorEl] = useState(null);
  
  // Toggle filter menu
  const handleFilterClick = (event) => {
    setFilterAnchorEl(event.currentTarget);
  };
  
  const handleFilterClose = () => {
    setFilterAnchorEl(null);
  };
  
  // Handle status filter change
  const handleStatusFilterChange = (event) => {
    const { value } = event.target;
    setStatusFilter(typeof value === 'string' ? value.split(',') : value);
  };
  
  // Handle frequency filter change
  const handleFrequencyFilterChange = (event) => {
    const { value } = event.target;
    setFrequencyFilter(typeof value === 'string' ? value.split(',') : value);
  };
  
  // Clear all filters
  const clearAllFilters = () => {
    setSearchTerm('');
    setStatusFilter([]);
    setFrequencyFilter([]);
  };
  
  // Handle search input change
  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
  };
  
  // Filtered jobs based on search and filters
  const filteredJobs = useMemo(() => {
    if (!scheduledJobs) return [];
    
    return scheduledJobs.filter(job => {
      // Filter by search term
      if (searchTerm && !job.MAP_REFERENCE.toLowerCase().includes(searchTerm.toLowerCase())) {
        return false;
      }
      
      // Filter by status
      if (statusFilter.length > 0 && !statusFilter.includes(job.STATUS)) {
        return false;
      }
      
      // Filter by frequency
      if (frequencyFilter.length > 0 && !frequencyFilter.includes(job.FREQUENCY_CODE)) {
        return false;
      }
      
      return true;
    });
  }, [scheduledJobs, searchTerm, statusFilter, frequencyFilter]);
  
  // Get available status options from jobs data
  const statusOptions = useMemo(() => {
    if (!scheduledJobs) return [];
    
    const uniqueStatuses = [...new Set(scheduledJobs.map(job => job.STATUS))];
    return uniqueStatuses.filter(Boolean).map(status => ({
      value: status,
      label: status === 'A' ? 'Active' : status === 'I' ? 'Inactive' : status
    }));
  }, [scheduledJobs]);
  
  // Get available frequency options from jobs data
  const frequencyOptions = useMemo(() => {
    if (!scheduledJobs) return [];
    
    const uniqueFrequencies = [...new Set(scheduledJobs.map(job => job.FREQUENCY_CODE))];
    
    // Map frequency codes to more readable names
    const frequencyLabels = {
      'DLY': 'Daily',
      'WKL': 'Weekly',
      'MTH': 'Monthly',
      'FTN': 'Fortnightly',
      'YRL': 'Yearly',
      'YR': 'Yearly',
      'HYL': 'Half-yearly',
      'HY': 'Half-yearly',
      'ID': 'Intraday',
      'ADH': 'Ad-hoc'
    };
    
    return uniqueFrequencies.filter(Boolean).map(freq => ({
      value: freq,
      label: frequencyLabels[freq] || freq
    }));
  }, [scheduledJobs]);

  // Function to format schedule details
  const formatScheduleDetails = (job) => {
    if (!job) return 'Not scheduled';

    const { FREQUENCY_CODE, FREQUENCY_DAY, FREQUENCY_HOUR, FREQUENCY_MINUTE } = job;
    
    // Return early if all frequency values are missing
    if (!FREQUENCY_CODE) {
      return 'Not specified';
    }
    
    // Format time properly with leading zeros
    const formatTime = (hour, minute) => {
      const formattedHour = hour ? hour.toString().padStart(2, '0') : '00';
      const formattedMinute = minute ? minute.toString().padStart(2, '0') : '00';
      return `${formattedHour}:${formattedMinute}`;
    };
    
    const time = formatTime(FREQUENCY_HOUR, FREQUENCY_MINUTE);
    let scheduleText = '';
    
    // Format based on frequency code
    switch (FREQUENCY_CODE) {
      case 'DLY': // Daily
        scheduleText = `Daily at ${time}`;
        break;
      case 'WKL': // Weekly
        // Map numeric day to name
        const dayMap = {
          '01': 'Monday',
          '02': 'Tuesday',
          '03': 'Wednesday',
          '04': 'Thursday',
          '05': 'Friday',
          '06': 'Saturday',
          '07': 'Sunday',
          '1': 'Monday',
          '2': 'Tuesday',
          '3': 'Wednesday',
          '4': 'Thursday',
          '5': 'Friday',
          '6': 'Saturday',
          '7': 'Sunday',
          'MON': 'Monday',
          'TUE': 'Tuesday',
          'WED': 'Wednesday',
          'THU': 'Thursday',
          'FRI': 'Friday',
          'SAT': 'Saturday',
          'SUN': 'Sunday'
        };
        const dayName = dayMap[FREQUENCY_DAY] || 'Sunday';
        scheduleText = `Weekly on ${dayName} at ${time}`;
        break;
      case 'MTH': // Monthly
        const day = FREQUENCY_DAY ? FREQUENCY_DAY.toString().padStart(2, '0') : '01';
        scheduleText = `Monthly on day ${day} at ${time}`;
        break;
      case 'FTN': // Fortnightly
        scheduleText = `Fortnightly on day ${FREQUENCY_DAY || '01'} at ${time}`;
        break;
      case 'YRL':
      case 'YR': // Yearly
        scheduleText = `Yearly on day ${FREQUENCY_DAY || '01'} at ${time}`;
        break;
      case 'HYL':
      case 'HY': // Half-yearly
        scheduleText = `Half-yearly on day ${FREQUENCY_DAY || '01'} at ${time}`;
        break;
      case 'ID': // Intraday
        const minute = FREQUENCY_MINUTE ? FREQUENCY_MINUTE.toString().padStart(2, '0') : '00';
        scheduleText = `Intraday every ${minute} min`;
        break;
      case 'ADH': // Ad-hoc
        scheduleText = 'Ad-hoc (on demand)';
        break;
      default:
        // Display all information we have
        let details = [];
        if (FREQUENCY_CODE) details.push(`Code: ${FREQUENCY_CODE}`);
        if (FREQUENCY_DAY) details.push(`Day: ${FREQUENCY_DAY}`);
        if (FREQUENCY_HOUR) details.push(`Hour: ${FREQUENCY_HOUR}`);
        if (FREQUENCY_MINUTE) details.push(`Min: ${FREQUENCY_MINUTE}`);
        
        scheduleText = details.length > 0 ? details.join(', ') : 'Not specified';
    }
    
    return scheduleText;
  };

  // Function to view job logs
  const handleViewLogs = async (mapReference) => {
    if (!mapReference) {
      console.error('Map reference is undefined or empty');
      return;
    }
    
    setSelectedMapRef(mapReference);
    await fetchJobLogs(mapReference);
    setOpenLogDialog(true);
  };

  // Function to view error details
  const handleViewErrors = async (jobId) => {
    if (!jobId) {
      console.error('Job ID is undefined or empty');
      return;
    }
    
    setSelectedJobId(jobId);
    await fetchErrorDetails(jobId);
    setOpenErrorDialog(true);
  };

  // Function to close log dialog
  const handleCloseLogDialog = () => {
    setOpenLogDialog(false);
  };

  // Function to close error dialog
  const handleCloseErrorDialog = () => {
    setOpenErrorDialog(false);
  };

  // Function to get status label
  const getStatusLabel = (status) => {
    if (!status) return 'Unknown';
    if (status === 'A') return 'Active';
    if (status === 'I') return 'Inactive';
    return status;
  };

  // Function to get status icon
  const getStatusIcon = (status) => {
    if (!status) return <ErrorIcon fontSize="small" />;
    if (status === 'A') return null;
    if (status === 'I') return <ErrorIcon fontSize="small" />;
    return null;
  };

  // Function to refresh data
  const handleRefresh = async () => {
    setIsRefreshing(true);
    await fetchScheduledJobs();
    setIsRefreshing(false);
  };

  // Get frequency icon based on frequency code
  const getFrequencyIcon = (frequencyCode) => {
    switch (frequencyCode) {
      case 'DLY':
        return <TodayIcon fontSize="small" />;
      case 'WKL':
        return <EventIcon fontSize="small" />;
      case 'MTH':
        return <CalendarMonthIcon fontSize="small" />;
      case 'ADH':
        return <UpdateIcon fontSize="small" />;
      default:
        return <AccessTimeIcon fontSize="small" />;
    }
  };

  // Filter out jobs with invalid data
  const validJobs = filteredJobs.filter(job => job && job.MAP_REFERENCE);
  
  // Count active filters
  const activeFilterCount = (statusFilter.length > 0 ? 1 : 0) + 
                           (frequencyFilter.length > 0 ? 1 : 0) +
                           (searchTerm ? 1 : 0);

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <Box p={{ xs: 2, sm: 3 }}>        
        <AnimatePresence>
          {error && (
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              transition={{ duration: 0.2 }}
            >
              <Alert 
                severity="error" 
                sx={{ 
                  mb: 3, 
                  borderRadius: 2,
                  boxShadow: darkMode ? '0 4px 12px rgba(0, 0, 0, 0.15)' : '0 2px 8px rgba(0, 0, 0, 0.05)'
                }}
              >
                {error}
              </Alert>
            </motion.div>
          )}
        </AnimatePresence>
        
        <Paper 
          elevation={darkMode ? 2 : 1} 
          sx={{ 
            backgroundColor: darkMode ? '#1A202C' : '#FFFFFF',
            borderRadius: '12px',
            overflow: 'hidden',
            mb: 4,
            boxShadow: darkMode ? '0 4px 12px rgba(0, 0, 0, 0.2)' : '0 2px 10px rgba(0, 0, 0, 0.05)',
            transition: 'all 0.3s ease'
          }}
        >
          <CardHeader darkMode={darkMode}>
            <Box display="flex" alignItems="center">
              <Typography variant="h6" component="h2" fontWeight={600}>
                Scheduled Jobs
              </Typography>
              <Badge 
                color="primary" 
                badgeContent={validJobs.length} 
                max={99}
                sx={{ ml: 1.5 }}
              />
            </Box>
            <Tooltip title="Refresh data">
              <RefreshIconButton 
                size="small" 
                onClick={handleRefresh} 
                darkMode={darkMode}
                disabled={loading || isRefreshing}
              >
                <RefreshIcon fontSize="small" 
                  sx={{ 
                    animation: isRefreshing ? 'spin 1s linear infinite' : 'none',
                    '@keyframes spin': {
                      '0%': { transform: 'rotate(0deg)' },
                      '100%': { transform: 'rotate(360deg)' }
                    }
                  }} 
                />
              </RefreshIconButton>
            </Tooltip>
          </CardHeader>
          
          {/* Search and Filter Section */}
          <Box 
            sx={{ 
              p: 2, 
              borderBottom: darkMode ? '1px solid rgba(255, 255, 255, 0.1)' : '1px solid rgba(0, 0, 0, 0.1)',
              display: 'flex',
              flexWrap: 'wrap',
              alignItems: 'center',
              gap: 1.5
            }}
          >
            {/* Search Bar */}
            <SearchBar
              placeholder="Search by map reference..."
              value={searchTerm}
              onChange={handleSearchChange}
              darkMode={darkMode}
              size="small"
              sx={{ flexGrow: 1, minWidth: { xs: '100%', sm: '200px' } }}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
                endAdornment: searchTerm ? (
                  <InputAdornment position="end">
                    <IconButton 
                      size="small" 
                      onClick={() => setSearchTerm('')}
                      edge="end"
                      aria-label="clear search"
                    >
                      <ClearIcon fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                ) : null
              }}
            />
            
            {/* Filter Button */}
            <FilterButton
              startIcon={<FilterAltIcon />}
              onClick={handleFilterClick}
              darkMode={darkMode}
              active={activeFilterCount > 0}
              aria-haspopup="true"
              aria-expanded={Boolean(filterAnchorEl)}
            >
              Filters {activeFilterCount > 0 && `(${activeFilterCount})`}
            </FilterButton>
            
            {/* Filter Menu */}
            <Menu
              anchorEl={filterAnchorEl}
              open={Boolean(filterAnchorEl)}
              onClose={handleFilterClose}
              PaperProps={{
                sx: {
                  mt: 1.5,
                  width: 280,
                  maxHeight: 500,
                  borderRadius: 2,
                  boxShadow: darkMode ? '0 4px 20px rgba(0, 0, 0, 0.3)' : '0 4px 20px rgba(0, 0, 0, 0.15)',
                  backgroundColor: darkMode ? '#1A202C' : '#FFFFFF'
                }
              }}
            >
              <Box sx={{ p: 2 }}>
                <Typography variant="subtitle1" fontWeight={600} gutterBottom>
                  Filter Jobs
                </Typography>
                
                {/* Status Filter */}
                <FormControl fullWidth margin="dense" size="small">
                  <InputLabel id="status-filter-label">Status</InputLabel>
                  <Select
                    labelId="status-filter-label"
                    id="status-filter"
                    multiple
                    value={statusFilter}
                    onChange={handleStatusFilterChange}
                    input={<OutlinedInput label="Status" />}
                    renderValue={(selected) => (
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {selected.map((value) => (
                          <Chip 
                            key={value} 
                            label={value === 'A' ? 'Active' : value === 'I' ? 'Inactive' : value} 
                            size="small"
                          />
                        ))}
                      </Box>
                    )}
                    MenuProps={{
                      PaperProps: {
                        style: {
                          maxHeight: 224,
                          width: 250,
                        },
                      },
                    }}
                  >
                    {statusOptions.map((option) => (
                      <MenuItem key={option.value} value={option.value}>
                        <Checkbox checked={statusFilter.indexOf(option.value) > -1} />
                        <ListItemText primary={option.label} />
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                
                {/* Frequency Filter */}
                <FormControl fullWidth margin="dense" size="small">
                  <InputLabel id="frequency-filter-label">Frequency</InputLabel>
                  <Select
                    labelId="frequency-filter-label"
                    id="frequency-filter"
                    multiple
                    value={frequencyFilter}
                    onChange={handleFrequencyFilterChange}
                    input={<OutlinedInput label="Frequency" />}
                    renderValue={(selected) => (
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {selected.map((value) => {
                          const option = frequencyOptions.find(opt => opt.value === value);
                          return (
                            <Chip 
                              key={value} 
                              label={option ? option.label : value} 
                              size="small"
                            />
                          );
                        })}
                      </Box>
                    )}
                    MenuProps={{
                      PaperProps: {
                        style: {
                          maxHeight: 224,
                          width: 250,
                        },
                      },
                    }}
                  >
                    {frequencyOptions.map((option) => (
                      <MenuItem key={option.value} value={option.value}>
                        <Checkbox checked={frequencyFilter.indexOf(option.value) > -1} />
                        <ListItemText primary={option.label} />
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                
                {/* Filter actions */}
                <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
                  <Button onClick={clearAllFilters} sx={{ mr: 1 }}>
                    Clear All
                  </Button>
                  <Button 
                    variant="contained" 
                    disableElevation
                    onClick={handleFilterClose}
                  >
                    Apply
                  </Button>
                </Box>
              </Box>
            </Menu>
            
            {/* Active Filter Chips */}
            {(statusFilter.length > 0 || frequencyFilter.length > 0) && (
              <Box sx={{ 
                display: 'flex', 
                flexWrap: 'wrap', 
                mt: { xs: 1, sm: 0 }, 
                width: { xs: '100%', sm: 'auto' } 
              }}>
                {statusFilter.map(status => (
                  <FilterChip
                    key={`status-${status}`}
                    label={`${status === 'A' ? 'Active' : status === 'I' ? 'Inactive' : status}`}
                    darkMode={darkMode}
                    onDelete={() => setStatusFilter(statusFilter.filter(s => s !== status))}
                    size="small"
                  />
                ))}
                
                {frequencyFilter.map(freq => {
                  const option = frequencyOptions.find(opt => opt.value === freq);
                  return (
                    <FilterChip
                      key={`freq-${freq}`}
                      label={option ? option.label : freq}
                      darkMode={darkMode}
                      onDelete={() => setFrequencyFilter(frequencyFilter.filter(f => f !== freq))}
                      size="small"
                    />
                  );
                })}
                
                {(statusFilter.length > 0 || frequencyFilter.length > 0) && (
                  <Button 
                    size="small" 
                    onClick={clearAllFilters}
                    sx={{ 
                      ml: 1, 
                      color: darkMode ? alpha('#FFFFFF', 0.7) : alpha('#000000', 0.7),
                      textTransform: 'none',
                    }}
                  >
                    Clear all
                  </Button>
                )}
              </Box>
            )}
          </Box>
          
          {(loading || isRefreshing) && (
            <LinearProgress
              sx={{
                height: 3,
                '& .MuiLinearProgress-bar': {
                  transition: 'transform 0.4s linear'
                }
              }}
            />
          )}
          
          {loading ? (
            <Box p={3}>
              <Stack spacing={1.5}>
                {[...Array(4)].map((_, i) => (
                  <Skeleton 
                    key={i} 
                    animation="wave" 
                    height={56} 
                    sx={{ 
                      borderRadius: '8px',
                      opacity: 1 - (i * 0.2),
                      bgcolor: darkMode ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.05)'
                    }} 
                  />
                ))}
              </Stack>
            </Box>
          ) : validJobs.length === 0 ? (
            <Box 
              p={4} 
              textAlign="center" 
              sx={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                minHeight: '200px'
              }}
            >
              <motion.div
                initial={{ scale: 0.8, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                transition={{ duration: 0.3 }}
              >
                <InfoIcon 
                  sx={{ 
                    fontSize: 56, 
                    color: 'text.secondary', 
                    opacity: 0.5, 
                    mb: 2,
                    color: darkMode ? 'primary.light' : 'primary.main'
                  }} 
                />
              </motion.div>
              <Typography 
                variant="h6" 
                color="text.secondary"
                sx={{ fontWeight: 500 }}
              >
                {searchTerm || statusFilter.length > 0 || frequencyFilter.length > 0 
                  ? "No jobs match your search criteria" 
                  : "No scheduled jobs found"}
              </Typography>
              <Typography 
                variant="body2" 
                color="text.secondary" 
                mt={1}
                sx={{ maxWidth: '400px' }}
              >
                {searchTerm || statusFilter.length > 0 || frequencyFilter.length > 0 
                  ? "Try adjusting your search or clearing filters to see more results." 
                  : "Jobs will appear here once they are scheduled in the system. Try creating and scheduling a job first."}
              </Typography>
              
              {(searchTerm || statusFilter.length > 0 || frequencyFilter.length > 0) && (
                <Button
                  variant="outlined"
                  color="primary"
                  sx={{ mt: 2 }}
                  onClick={clearAllFilters}
                >
                  Clear All Filters
                </Button>
              )}
            </Box>
          ) : (
            <StyledTableContainer darkMode={darkMode}>
              <Table stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell width="40%">Map Reference</TableCell>
                    <TableCell width="15%">Status</TableCell>
                    <TableCell width="25%">Schedule</TableCell>
                    <TableCell align="center" width="20%">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {validJobs.map((job, index) => (
                    <TableRow 
                      key={index}
                      sx={{ 
                        cursor: 'pointer',
                        '&:last-child td, &:last-child th': { border: 0 }
                      }}
                      onClick={() => job.MAP_REFERENCE && handleViewLogs(job.MAP_REFERENCE)}
                      component={motion.tr}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ duration: 0.3, delay: index * 0.05 }}
                    >
                      <TableCell>
                        <Typography variant="body2" fontWeight={500}>
                          {job.MAP_REFERENCE || 'N/A'}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <StatusChip
                          status={job.STATUS}
                          darkMode={darkMode}
                          label={getStatusLabel(job.STATUS)}
                          size="small"
                          icon={getStatusIcon(job.STATUS)}
                        />
                      </TableCell>
                      <TableCell>
                        <Tooltip 
                          title={
                            <Box sx={{ p: 0.5 }}>
                              <Typography variant="subtitle2" fontWeight={600}>Schedule Details</Typography>
                              <Typography variant="body2" sx={{ mt: 0.5 }}>
                                {job.FREQUENCY_CODE ? `Frequency: ${job.FREQUENCY_CODE}` : 'Frequency: Not set'}
                              </Typography>
                              {job.FREQUENCY_DAY && (
                                <Typography variant="body2">
                                  Day: {job.FREQUENCY_DAY}
                                </Typography>
                              )}
                              {(job.FREQUENCY_HOUR || job.FREQUENCY_MINUTE) && (
                                <Typography variant="body2">
                                  Time: {job.FREQUENCY_HOUR || '00'}:{job.FREQUENCY_MINUTE || '00'}
                                </Typography>
                              )}
                            </Box>
                          } 
                          arrow
                        >
                          <ScheduleChip darkMode={darkMode} frequencyType={job.FREQUENCY_CODE}>
                            {getFrequencyIcon(job.FREQUENCY_CODE)}
                            <Typography variant="body2" component="span" sx={{ fontWeight: 500 }}>
                              {formatScheduleDetails(job)}
                            </Typography>
                          </ScheduleChip>
                        </Tooltip>
                      </TableCell>
                      <TableCell align="center">
                        {job.MAP_REFERENCE ? (
                          <Tooltip title="View Logs">
                            <ActionButton
                              darkMode={darkMode}
                              onClick={(e) => {
                                e.stopPropagation();
                                handleViewLogs(job.MAP_REFERENCE);
                              }}
                            >
                              <VisibilityIcon fontSize="small" />
                            </ActionButton>
                          </Tooltip>
                        ) : (
                          <Tooltip title="Invalid job data">
                            <span>
                              <ActionButton
                                darkMode={darkMode}
                                disabled
                              >
                                <ErrorIcon fontSize="small" />
                              </ActionButton>
                            </span>
                          </Tooltip>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </StyledTableContainer>
          )}
        </Paper>

        {/* Log Details Dialog */}
        <LogDetailsDialog
          open={openLogDialog}
          onClose={handleCloseLogDialog}
          mapReference={selectedMapRef}
          logDetails={logDetails}
          loading={logLoading}
          darkMode={darkMode}
          onViewErrors={handleViewErrors}
        />

        {/* Error Details Dialog */}
        <ErrorDetailsDialog
          open={openErrorDialog}
          onClose={handleCloseErrorDialog}
          errorDetails={errorDetails}
          jobId={selectedJobId}
          loading={errorLoading}
          darkMode={darkMode}
        />
      </Box>
    </motion.div>
  );
};

export default JobStatusAndLogsPage;
