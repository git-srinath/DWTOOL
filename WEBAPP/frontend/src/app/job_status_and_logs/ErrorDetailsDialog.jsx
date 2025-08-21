import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Typography,
  Box,
  IconButton,
  Button,
  CircularProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Grid,
  Divider,
  Paper,
  Chip,
  useMediaQuery
} from '@mui/material';
import { styled, alpha, useTheme as useMuiTheme } from '@mui/material/styles';
import {
  Close as CloseIcon,
  ExpandMore as ExpandMoreIcon,
  Error as ErrorIcon,
  Info as InfoIcon,
  BugReport as BugReportIcon,
  Schedule as ScheduleIcon,
  Key as KeyIcon,
  Code as CodeIcon,
  Storage as StorageIcon
} from '@mui/icons-material';
import { format } from 'date-fns';
import { motion } from 'framer-motion';

// Format date safely
const formatDate = (dateStr, formatString = 'yyyy-MM-dd HH:mm:ss') => {
  if (!dateStr) return '-';
  try {
    return format(new Date(dateStr), formatString);
  } catch (error) {
    console.error(`Error formatting date: ${dateStr}`, error);
    return dateStr || '-';
  }
};

const StyledDialog = styled(Dialog)(({ theme, darkMode }) => ({
  '& .MuiDialog-paper': {
    backgroundColor: darkMode ? '#1A202C' : '#FFFFFF',
    backgroundImage: 'none',
    boxShadow: darkMode ? '0 4px 20px rgba(0, 0, 0, 0.5)' : '0 4px 20px rgba(0, 0, 0, 0.15)',
    borderRadius: '12px',
    overflow: 'hidden',
    transition: 'all 0.3s ease'
  }
}));

const ErrorHeader = styled(DialogTitle)(({ theme, darkMode }) => ({
  borderBottom: darkMode ? '1px solid rgba(255, 255, 255, 0.1)' : '1px solid rgba(0, 0, 0, 0.1)',
  backgroundColor: darkMode ? theme.palette.error.dark : theme.palette.error.light,
  color: darkMode ? '#FFFFFF' : theme.palette.error.dark,
  padding: theme.spacing(2),
}));

const EmptyStateContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  padding: theme.spacing(6),
  textAlign: 'center',
  minHeight: '300px'
}));

const StyledAccordion = styled(Accordion)(({ theme, darkMode, isFirst }) => ({
  backgroundColor: 'transparent',
  boxShadow: 'none',
  border: darkMode ? '1px solid rgba(255, 255, 255, 0.1)' : '1px solid rgba(0, 0, 0, 0.1)',
  borderRadius: '8px !important',
  marginBottom: theme.spacing(2),
  overflow: 'hidden',
  '&:before': {
    display: 'none',
  },
  '&.Mui-expanded': {
    margin: isFirst ? theme.spacing(0, 0, 2, 0) : theme.spacing(0, 0, 2, 0),
    backgroundColor: darkMode ? alpha(theme.palette.error.dark, 0.1) : alpha(theme.palette.error.light, 0.1),
  },
  transition: 'all 0.2s ease',
}));

const StyledAccordionSummary = styled(AccordionSummary)(({ theme, darkMode }) => ({
  backgroundColor: darkMode ? alpha(theme.palette.error.dark, 0.05) : alpha(theme.palette.error.light, 0.05),
  '&.Mui-expanded': {
    backgroundColor: darkMode ? alpha(theme.palette.error.dark, 0.1) : alpha(theme.palette.error.light, 0.1),
  },
  '& .MuiAccordionSummary-expandIconWrapper': {
    transition: 'transform 0.3s ease',
  },
  padding: theme.spacing(1, 2),
}));

const ErrorTypeChip = styled(Chip)(({ theme, darkMode }) => ({
  backgroundColor: darkMode ? alpha(theme.palette.error.main, 0.2) : alpha(theme.palette.error.main, 0.1),
  color: darkMode ? theme.palette.error.light : theme.palette.error.dark,
  fontWeight: 500,
  fontSize: '0.75rem',
  borderRadius: '16px',
  border: `1px solid ${darkMode ? alpha(theme.palette.error.main, 0.3) : alpha(theme.palette.error.main, 0.2)}`,
}));

const ErrorMessagePaper = styled(Paper)(({ theme, darkMode }) => ({
  padding: theme.spacing(2),
  backgroundColor: darkMode ? alpha(theme.palette.error.dark, 0.05) : alpha(theme.palette.error.light, 0.05),
  color: darkMode ? alpha(theme.palette.error.light, 0.9) : theme.palette.error.dark,
  border: `1px solid ${darkMode ? alpha(theme.palette.error.dark, 0.2) : alpha(theme.palette.error.light, 0.3)}`,
  borderRadius: '8px',
  fontFamily: 'monospace',
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-word',
  fontSize: '0.875rem',
  lineHeight: 1.6,
  position: 'relative',
  overflow: 'hidden',
  '&::before': {
    content: '""',
    position: 'absolute',
    top: 0,
    left: 0,
    width: '4px',
    height: '100%',
    backgroundColor: darkMode ? theme.palette.error.main : theme.palette.error.main,
  }
}));

const InfoItem = styled(Box)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(0.5),
}));

const InfoLabel = styled(Typography)(({ theme, darkMode }) => ({
  display: 'flex',
  alignItems: 'center',
  gap: theme.spacing(0.5),
  fontSize: '0.75rem',
  fontWeight: 500,
  color: darkMode ? alpha(theme.palette.common.white, 0.7) : alpha(theme.palette.common.black, 0.6),
  marginBottom: theme.spacing(0.5),
}));

const InfoValue = styled(Typography)(({ theme }) => ({
  fontSize: '0.875rem',
  fontWeight: 400,
  wordBreak: 'break-word',
}));

const IconWrapper = styled(Box)(({ theme, darkMode, color }) => ({
  width: 24,
  height: 24,
  borderRadius: '50%',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  backgroundColor: darkMode 
    ? alpha(theme.palette[color || 'error'].main, 0.2) 
    : alpha(theme.palette[color || 'error'].main, 0.1),
  color: darkMode 
    ? theme.palette[color || 'error'].light 
    : theme.palette[color || 'error'].main,
  marginRight: theme.spacing(1),
}));

const ErrorDetailsDialog = ({ 
  open, 
  onClose, 
  errorDetails, 
  jobId, 
  loading, 
  darkMode 
}) => {
  const muiTheme = useMuiTheme();
  const isMobile = useMediaQuery(muiTheme.breakpoints.down('sm'));
  
  // Make sure errorDetails is an array
  const errors = Array.isArray(errorDetails) ? errorDetails : [];
  
  // Filter out any invalid error objects
  const validErrors = errors.filter(error => error && typeof error === 'object');
  
  return (
    <StyledDialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      darkMode={darkMode}
    >
      <ErrorHeader darkMode={darkMode}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Box display="flex" alignItems="center">
            <IconWrapper darkMode={darkMode}>
              <ErrorIcon fontSize="small" />
            </IconWrapper>
            <Typography variant="h6" component="span" fontWeight={600}>
              Error Details for Job ID: {jobId || 'Unknown'}
            </Typography>
          </Box>
          <IconButton
            edge="end"
            color="inherit"
            onClick={onClose}
            aria-label="close"
            sx={{ 
              transition: 'transform 0.2s ease',
              '&:hover': { transform: 'rotate(90deg)' }
            }}
          >
            <CloseIcon />
          </IconButton>
        </Box>
      </ErrorHeader>
      
      <DialogContent sx={{ p: 3 }}>
        {loading ? (
          <EmptyStateContainer>
            <CircularProgress size={40} sx={{ mb: 2 }} color="error" />
            <Typography variant="subtitle1" color="text.secondary">
              Loading error details...
            </Typography>
          </EmptyStateContainer>
        ) : validErrors.length === 0 ? (
          <EmptyStateContainer>
            <InfoIcon sx={{ fontSize: 64, color: 'text.secondary', opacity: 0.5, mb: 2 }} />
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No error details found
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ maxWidth: '450px' }}>
              No specific error information is available for this job. This could mean the job failed without generating detailed error records.
            </Typography>
          </EmptyStateContainer>
        ) : (
          <Box>
            {validErrors.map((error, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.3, delay: index * 0.1 }}
              >
                <StyledAccordion 
                  defaultExpanded={index === 0} 
                  darkMode={darkMode} 
                  isFirst={index === 0}
                >
                  <StyledAccordionSummary 
                    expandIcon={<ExpandMoreIcon />}
                    darkMode={darkMode}
                  >
                    <Box display="flex" alignItems="center" width="100%">
                      <Box display="flex" alignItems="center" flexGrow={1}>
                        <IconWrapper darkMode={darkMode}>
                          <BugReportIcon fontSize="small" />
                        </IconWrapper>
                        <Typography fontWeight={500}>
                          Error #{error.ERROR_ID || index + 1}
                        </Typography>
                      </Box>
                      {error.ERROR_TYPE && (
                        <ErrorTypeChip 
                          label={error.ERROR_TYPE || 'Unknown Error Type'} 
                          size="small"
                          darkMode={darkMode}
                        />
                      )}
                    </Box>
                  </StyledAccordionSummary>
                  <AccordionDetails sx={{ p: 3 }}>
                    <Grid container spacing={3}>
                      {/* Error metadata */}
                      <Grid item xs={12} container spacing={2}>
                        <Grid item xs={12} sm={6}>
                          <InfoItem>
                            <InfoLabel darkMode={darkMode}>
                              <IconWrapper darkMode={darkMode} color="primary">
                                <ScheduleIcon fontSize="small" />
                              </IconWrapper>
                              Process Date
                            </InfoLabel>
                            <InfoValue>
                              {formatDate(error.PROCESS_DATE)}
                            </InfoValue>
                          </InfoItem>
                        </Grid>
                        <Grid item xs={12} sm={6}>
                          <InfoItem>
                            <InfoLabel darkMode={darkMode}>
                              <IconWrapper darkMode={darkMode} color="primary">
                                <KeyIcon fontSize="small" />
                              </IconWrapper>
                              Key Value
                            </InfoLabel>
                            <InfoValue>
                              {error.KEY_VALUE || '-'}
                            </InfoValue>
                          </InfoItem>
                        </Grid>
                      </Grid>
                      
                      <Grid item xs={12}>
                        <Divider sx={{ my: 1 }} />
                        <InfoLabel darkMode={darkMode} sx={{ mb: 1 }}>
                          <IconWrapper darkMode={darkMode} color="error">
                            <CodeIcon fontSize="small" />
                          </IconWrapper>
                          Error Message
                        </InfoLabel>
                        <ErrorMessagePaper darkMode={darkMode}>
                          {error.ERROR_MESSAGE || 'No error message available'}
                        </ErrorMessagePaper>
                      </Grid>
                      
                      {error.DATABASE_ERROR_MESSAGE && (
                        <Grid item xs={12}>
                          <InfoLabel darkMode={darkMode} sx={{ mt: 1, mb: 1 }}>
                            <IconWrapper darkMode={darkMode} color="error">
                              <StorageIcon fontSize="small" />
                            </IconWrapper>
                            Database Error Message
                          </InfoLabel>
                          <ErrorMessagePaper darkMode={darkMode}>
                            {error.DATABASE_ERROR_MESSAGE}
                          </ErrorMessagePaper>
                        </Grid>
                      )}
                    </Grid>
                  </AccordionDetails>
                </StyledAccordion>
              </motion.div>
            ))}
          </Box>
        )}
      </DialogContent>
      
      <DialogActions sx={{ p: 2, borderTop: darkMode ? '1px solid rgba(255, 255, 255, 0.1)' : '1px solid rgba(0, 0, 0, 0.1)' }}>
        <Button 
          onClick={onClose} 
          color="primary" 
          variant="outlined"
          startIcon={<CloseIcon />}
        >
          Close
        </Button>
      </DialogActions>
    </StyledDialog>
  );
};

export default ErrorDetailsDialog; 