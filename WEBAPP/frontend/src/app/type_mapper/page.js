'use client'

import { useState, useEffect } from 'react'
import axios from 'axios'
import { format } from 'date-fns'
// Import Material UI components
import {
  Box,
  Typography,
  Container,
  Paper,
  Button,
  TextField,
  MenuItem,
  Grid,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Alert,
  IconButton,
  CircularProgress,
  Chip,
  Divider,
  TablePagination,
  useTheme,
  Card,
  CardContent,
  Tooltip,
  Fade,
  alpha,
} from '@mui/material'
import {
  Add as AddIcon,
  Cancel as CancelIcon,
  FilterList as FilterListIcon,
  Close as CloseIcon,
  Save as SaveIcon,
  Info as InfoIcon,
  Refresh as RefreshIcon,
  Settings as SettingsIcon,
} from '@mui/icons-material'

export default function ParameterPage() {
  const theme = useTheme()
  const [parameters, setParameters] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [showAddForm, setShowAddForm] = useState(false)
  const [filterType, setFilterType] = useState('')
  const [types, setTypes] = useState([])

  // Pagination state
  const [page, setPage] = useState(0)
  const [rowsPerPage, setRowsPerPage] = useState(10)

  // Form state
  const [newParameter, setNewParameter] = useState({
    PRTYP: '',
    PRCD: '',
    PRDESC: '',
    PRVAL: '',
  })

  useEffect(() => {
    fetchParameters()
  }, [])

  const fetchParameters = async () => {
    try {
      setLoading(true)
      const response = await axios.get(
        `${process.env.NEXT_PUBLIC_API_URL}/mapping/parameter_mapping`
      )
      setParameters(response.data)

      // Extract unique parameter types for filtering
      const uniqueTypes = [
        ...new Set(response.data.map((param) => param.PRTYP)),
      ]
      setTypes(uniqueTypes)

      setLoading(false)
    } catch (err) {
      setError('Failed to load parameters. Please try again later.')
      setLoading(false)
    }
  }

  const handleAddParameter = async (e) => {
    e.preventDefault()

    try {
      setLoading(true)
      await axios.post(
        `${process.env.NEXT_PUBLIC_API_URL}/mapping/parameter_add`,
        newParameter
      )

      // Reset form and refresh data
      setNewParameter({
        PRTYP: '',
        PRCD: '',
        PRDESC: '',
        PRVAL: '',
      })
      setShowAddForm(false)
      fetchParameters()
    } catch (err) {
      setError('Failed to add parameter. Please try again.')
      setLoading(false)
    }
  }

  const handleChange = (e) => {
    const { name, value } = e.target
    setNewParameter((prev) => ({
      ...prev,
      [name]: value,
    }))
  }

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A'
    try {
      const date = new Date(dateString)
      return format(date, 'MMM dd, yyyy HH:mm')
    } catch (err) {
      return dateString.toString()
    }
  }

  // Handle pagination changes
  const handleChangePage = (event, newPage) => {
    setPage(newPage)
  }

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10))
    setPage(0)
  }

  const filteredParameters = filterType
    ? parameters.filter((param) => param.PRTYP === filterType)
    : parameters

  // Create paginated parameters
  const paginatedParameters = filteredParameters.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage
  )

  return (
    <Box
      sx={{
        minHeight: '100vh',
        background:
          theme.palette.mode === 'dark'
            ? `linear-gradient(${alpha(
                theme.palette.primary.dark,
                0.05
              )}, ${alpha(theme.palette.background.default, 1)})`
            : `linear-gradient(${alpha(
                theme.palette.primary.light,
                0.05
              )}, ${alpha(theme.palette.background.default, 1)})`,
        py: 2,
        px: 1.5,
      }}
    >
      <Box maxWidth="100%" sx={{ mx: 'auto' }}>
        <Card
          elevation={0}
          sx={{
            mb: 3,
            borderRadius: 2,
            backgroundColor:
              theme.palette.mode === 'dark'
                ? alpha(theme.palette.background.paper, 0.8)
                : 'white',
            border: `1px solid ${alpha(
              theme.palette.divider,
              theme.palette.mode === 'dark' ? 0.2 : 0.1
            )}`,
            boxShadow: `0 4px 15px ${alpha(
              theme.palette.common.black,
              theme.palette.mode === 'dark' ? 0.3 : 0.05
            )}`,
          }}
        >
          <CardContent sx={{ p: { xs: 1.5, sm: 2.5 } }}>
            <Box
              sx={{
                display: 'flex',
                flexDirection: { xs: 'column', md: 'row' },
                justifyContent: 'space-between',
                alignItems: { xs: 'flex-start', md: 'center' },
                mb: 2,
                gap: 1.5,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                <SettingsIcon
                  sx={{ fontSize: 24, color: theme.palette.primary.main }}
                />
                <Box>
                  <Typography
                    variant="h5"
                    component="h1"
                    sx={{
                      fontWeight: 700,
                      background: `linear-gradient(120deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
                      WebkitBackgroundClip: 'text',
                      WebkitTextFillColor: 'transparent',
                      letterSpacing: '-0.5px',
                    }}
                  >
                    System Parameters
                  </Typography>
                  <Typography
                    variant="body2"
                    sx={{ 
                      color: theme.palette.text.secondary,
                      fontSize: '0.75rem'
                    }}
                  >
                    Manage global configuration settings that control
                    application behavior
                  </Typography>
                </Box>
              </Box>

              <Box
                sx={{
                  display: 'flex',
                  gap: 1.5,
                  alignItems: 'center',
                  flexWrap: { xs: 'wrap', sm: 'nowrap' },
                  width: { xs: '100%', md: 'auto' },
                }}
              >
                <TextField
                  select
                  size="small"
                  label="Filter by Type"
                  value={filterType}
                  onChange={(e) => setFilterType(e.target.value)}
                  sx={{
                    minWidth: { xs: '100%', sm: 180 },
                    backgroundColor: theme.palette.background.paper,
                    '& .MuiOutlinedInput-root': {
                      borderRadius: 1.5,
                    },
                  }}
                  InputProps={{
                    startAdornment: (
                      <FilterListIcon
                        fontSize="small"
                        sx={{ mr: 1, color: 'text.secondary' }}
                      />
                    ),
                  }}
                >
                  <MenuItem value="">All Types</MenuItem>
                  {types.map((type) => (
                    <MenuItem key={type} value={type}>
                      {type}
                    </MenuItem>
                  ))}
                </TextField>

                <Tooltip
                  title={loading ? 'Loading...' : 'Refresh Parameters'}
                  arrow
                >
                  <IconButton
                    color="primary"
                    onClick={fetchParameters}
                    disabled={loading}
                    size="small"
                    sx={{
                      backgroundColor: alpha(theme.palette.primary.main, 0.1),
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.2),
                      },
                    }}
                  >
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </Tooltip>

                <Button
                  variant={showAddForm ? 'outlined' : 'contained'}
                  startIcon={showAddForm ? <CancelIcon /> : <AddIcon />}
                  onClick={() => setShowAddForm(!showAddForm)}
                  color={showAddForm ? 'error' : 'primary'}
                  size="small"
                  sx={{
                    borderRadius: 1.5,
                    boxShadow: showAddForm
                      ? 'none'
                      : '0 3px 8px rgba(0,0,0,0.1)',
                    px: 2,
                  }}
                >
                  {showAddForm ? 'Cancel' : 'Add Parameter'}
                </Button>
              </Box>
            </Box>

            <Divider sx={{ mb: 2, opacity: 0.7 }} />

            {error && (
              <Alert
                severity="error"
                sx={{
                  mb: 2,
                  borderRadius: 1.5,
                  boxShadow: `0 3px 8px ${alpha(
                    theme.palette.error.main,
                    0.2
                  )}`,
                  py: 0.5,
                }}
                action={
                  <IconButton
                    aria-label="close"
                    color="inherit"
                    size="small"
                    onClick={() => setError(null)}
                  >
                    <CloseIcon fontSize="small" />
                  </IconButton>
                }
              >
                {error}
              </Alert>
            )}

            {showAddForm && (
              <Fade in={showAddForm}>
                <Card
                  elevation={2}
                  sx={{
                    p: { xs: 1.5, sm: 2 },
                    mb: 2.5,
                    backgroundColor: alpha(theme.palette.primary.light, 0.04),
                    borderRadius: 2,
                    border: `1px solid ${alpha(
                      theme.palette.primary.main,
                      0.15
                    )}`,
                  }}
                >
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 0.75,
                      mb: 1.5,
                    }}
                  >
                    <AddIcon 
                      sx={{ 
                        color: theme.palette.primary.main,
                        fontSize: '1.2rem'
                      }} 
                    />
                    <Typography
                      variant="subtitle1"
                      sx={{
                        fontWeight: 600,
                        color: theme.palette.primary.dark,
                      }}
                    >
                      Add New Parameter
                    </Typography>
                  </Box>
                  <form onSubmit={handleAddParameter}>
                    <Grid container spacing={2}>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Type"
                          name="PRTYP"
                          value={newParameter.PRTYP}
                          onChange={handleChange}
                          required
                          placeholder="DB, Datatype, etc."
                          variant="outlined"
                          size="small"
                          sx={{
                            '& .MuiOutlinedInput-root': {
                              borderRadius: 1.5,
                              backgroundColor: theme.palette.background.paper,
                            },
                          }}
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Code"
                          name="PRCD"
                          value={newParameter.PRCD}
                          onChange={handleChange}
                          required
                          placeholder="Parameter code"
                          variant="outlined"
                          size="small"
                          sx={{
                            '& .MuiOutlinedInput-root': {
                              borderRadius: 1.5,
                              backgroundColor: theme.palette.background.paper,
                            },
                          }}
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Description"
                          name="PRDESC"
                          value={newParameter.PRDESC}
                          onChange={handleChange}
                          required
                          placeholder="Parameter description"
                          variant="outlined"
                          size="small"
                          sx={{
                            '& .MuiOutlinedInput-root': {
                              borderRadius: 1.5,
                              backgroundColor: theme.palette.background.paper,
                            },
                          }}
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Value"
                          name="PRVAL"
                          value={newParameter.PRVAL}
                          onChange={handleChange}
                          required
                          placeholder="Parameter value"
                          variant="outlined"
                          size="small"
                          sx={{
                            '& .MuiOutlinedInput-root': {
                              borderRadius: 1.5,
                              backgroundColor: theme.palette.background.paper,
                            },
                          }}
                        />
                      </Grid>
                    </Grid>
                    <Box
                      sx={{
                        mt: 2,
                        display: 'flex',
                        justifyContent: 'flex-end',
                      }}
                    >
                      <Button
                        type="submit"
                        variant="contained"
                        color="success"
                        disabled={loading}
                        size="small"
                        startIcon={
                          loading ? (
                            <CircularProgress size={16} />
                          ) : (
                            <SaveIcon fontSize="small" />
                          )
                        }
                        sx={{
                          borderRadius: 1.5,
                          px: 2,
                          boxShadow: '0 3px 8px rgba(0,0,0,0.15)',
                        }}
                      >
                        {loading ? 'Saving...' : 'Save Parameter'}
                      </Button>
                    </Box>
                  </form>
                </Card>
              </Fade>
            )}

            {loading && !showAddForm ? (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center',
                  p: 5,
                }}
              >
                <CircularProgress size={40} thickness={4} />
              </Box>
            ) : (
              <Box sx={{ borderRadius: 2, overflow: 'hidden' }}>
                <TableContainer
                  component={Paper}
                  elevation={0}
                  sx={{
                    borderRadius: '12px 12px 0 0',
                    border: '1px solid',
                    borderColor: alpha(
                      theme.palette.divider,
                      theme.palette.mode === 'dark' ? 0.3 : 0.2
                    ),
                    borderBottom: 'none',
                    backgroundColor:
                      theme.palette.mode === 'dark'
                        ? alpha(theme.palette.background.paper, 0.4)
                        : theme.palette.background.paper,
                    overflow: 'auto',
                    '&::-webkit-scrollbar': {
                      height: '6px',
                      backgroundColor: alpha(theme.palette.divider, 0.05),
                    },
                    '&::-webkit-scrollbar-thumb': {
                      backgroundColor: alpha(theme.palette.primary.main, 0.2),
                      borderRadius: '3px',
                      '&:hover': {
                        backgroundColor: alpha(theme.palette.primary.main, 0.3),
                      },
                    },
                  }}
                >
                  <Table sx={{ minWidth: 800 }} size="small">
                    <TableHead
                      sx={{
                        backgroundColor:
                          theme.palette.mode === 'dark'
                            ? alpha(theme.palette.primary.dark, 0.2)
                            : alpha(theme.palette.primary.main, 0.05),
                      }}
                    >
                      <TableRow>
                        <TableCell sx={{ py: 1, px: 1.5 }}>
                          <Typography
                            variant="subtitle2"
                            sx={{
                              fontWeight: 700,
                              color: theme.palette.primary.dark,
                              fontSize: '0.7rem',
                            }}
                          >
                            Type
                          </Typography>
                        </TableCell>
                        <TableCell sx={{ py: 1, px: 1.5 }}>
                          <Typography
                            variant="subtitle2"
                            sx={{
                              fontWeight: 700,
                              color: theme.palette.primary.dark,
                              fontSize: '0.7rem',
                            }}
                          >
                            Code
                          </Typography>
                        </TableCell>
                        <TableCell sx={{ py: 1, px: 1.5 }}>
                          <Typography
                            variant="subtitle2"
                            sx={{
                              fontWeight: 700,
                              color: theme.palette.primary.dark,
                              fontSize: '0.7rem',
                            }}
                          >
                            Description
                          </Typography>
                        </TableCell>
                        <TableCell sx={{ py: 1, px: 1.5 }}>
                          <Typography
                            variant="subtitle2"
                            sx={{
                              fontWeight: 700,
                              color: theme.palette.primary.dark,
                              fontSize: '0.7rem',
                            }}
                          >
                            Value
                          </Typography>
                        </TableCell>
                        <TableCell sx={{ py: 1, px: 1.5 }}>
                          <Typography
                            variant="subtitle2"
                            sx={{
                              fontWeight: 700,
                              color: theme.palette.primary.dark,
                              fontSize: '0.7rem',
                            }}
                          >
                            Created
                          </Typography>
                        </TableCell>
                        <TableCell sx={{ py: 1, px: 1.5 }}>
                          <Typography
                            variant="subtitle2"
                            sx={{
                              fontWeight: 700,
                              color: theme.palette.primary.dark,
                              fontSize: '0.7rem',
                            }}
                          >
                            Updated
                          </Typography>
                        </TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {paginatedParameters.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                            <Box
                              sx={{
                                display: 'flex',
                                flexDirection: 'column',
                                alignItems: 'center',
                                gap: 1.5,
                              }}
                            >
                              <InfoIcon
                                sx={{
                                  fontSize: 36,
                                  color: alpha(
                                    theme.palette.text.secondary,
                                    0.5
                                  ),
                                }}
                              />
                              <Typography
                                color="text.secondary"
                                sx={{ fontSize: '0.9rem' }}
                              >
                                {parameters.length === 0
                                  ? 'No parameters found'
                                  : 'No parameters match the selected filter'}
                              </Typography>
                              {filterType && (
                                <Button
                                  variant="outlined"
                                  size="small"
                                  onClick={() => setFilterType('')}
                                  sx={{ mt: 0.5 }}
                                >
                                  Clear Filter
                                </Button>
                              )}
                            </Box>
                          </TableCell>
                        </TableRow>
                      ) : (
                        paginatedParameters.map((param, index) => (
                          <TableRow
                            key={index}
                            hover
                            sx={{
                              '&:last-child td, &:last-child th': { border: 0 },
                              transition: 'all 0.2s ease',
                              cursor: 'pointer',
                              '&:hover': {
                                backgroundColor:
                                  theme.palette.mode === 'dark'
                                    ? alpha(theme.palette.primary.main, 0.1)
                                    : alpha(theme.palette.primary.main, 0.02),
                                transform: 'translateY(-1px)',
                                boxShadow: `0 2px 4px ${alpha(
                                  theme.palette.common.black,
                                  theme.palette.mode === 'dark' ? 0.2 : 0.05
                                )}`,
                              },
                            }}
                          >
                            <TableCell sx={{ py: 1, px: 1.5 }}>
                              <Chip
                                label={param.PRTYP}
                                size="small"
                                sx={{
                                  backgroundColor: alpha(
                                    theme.palette.primary.main,
                                    0.1
                                  ),
                                  color: theme.palette.primary.dark,
                                  fontWeight: 600,
                                  borderRadius: '8px',
                                  height: '20px',
                                  '& .MuiChip-label': {
                                    px: 1,
                                    py: 0,
                                    fontSize: '0.7rem',
                                  },
                                }}
                              />
                            </TableCell>
                            <TableCell sx={{ py: 1, px: 1.5 }}>
                              <Typography
                                variant="body2"
                                sx={{ fontWeight: 600, fontSize: '0.8rem' }}
                              >
                                {param.PRCD}
                              </Typography>
                            </TableCell>
                            <TableCell sx={{ py: 1, px: 1.5 }}>
                              <Typography
                                variant="body2"
                                sx={{ lineHeight: 1.3, fontSize: '0.8rem' }}
                              >
                                {param.PRDESC}
                              </Typography>
                            </TableCell>
                            <TableCell sx={{ py: 1, px: 1.5 }}>
                              <Typography
                                variant="body2"
                                sx={{
                                  fontFamily: '"Roboto Mono", monospace',
                                  backgroundColor:
                                    theme.palette.mode === 'dark'
                                      ? alpha(theme.palette.grey[900], 0.5)
                                      : alpha(theme.palette.grey[100], 0.7),
                                  p: 0.3,
                                  px: 0.8,
                                  borderRadius: 1,
                                  display: 'inline-block',
                                  border: '1px solid',
                                  borderColor:
                                    theme.palette.mode === 'dark'
                                      ? alpha(theme.palette.grey[700], 0.5)
                                      : alpha(theme.palette.grey[300], 0.5),
                                  fontSize: '0.75rem',
                                  letterSpacing: '0.25px',
                                }}
                              >
                                {param.PRVAL}
                              </Typography>
                            </TableCell>
                            <TableCell sx={{ py: 1, px: 1.5 }}>
                              <Typography
                                variant="body2"
                                color="text.secondary"
                                sx={{
                                  whiteSpace: 'nowrap',
                                  fontSize: '0.75rem',
                                }}
                              >
                                {formatDate(param.PRRECCRDT)}
                              </Typography>
                            </TableCell>
                            <TableCell sx={{ py: 1, px: 1.5 }}>
                              <Typography
                                variant="body2"
                                color="text.secondary"
                                sx={{
                                  whiteSpace: 'nowrap',
                                  fontSize: '0.75rem',
                                }}
                              >
                                {formatDate(param.PRRECUPDT)}
                              </Typography>
                            </TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>

                {/* Pagination component */}
                <TablePagination
                  component="div"
                  count={filteredParameters.length}
                  page={page}
                  onPageChange={handleChangePage}
                  rowsPerPage={rowsPerPage}
                  onRowsPerPageChange={handleChangeRowsPerPage}
                  rowsPerPageOptions={[5, 10, 25, 50, 100]}
                  sx={{
                    borderRadius: '0 0 12px 12px',
                    backgroundColor:
                      theme.palette.mode === 'dark'
                        ? alpha(theme.palette.background.paper, 0.4)
                        : theme.palette.background.paper,
                    borderLeft: '1px solid',
                    borderRight: '1px solid',
                    borderBottom: '1px solid',
                    borderColor: alpha(
                      theme.palette.divider,
                      theme.palette.mode === 'dark' ? 0.3 : 0.2
                    ),
                    '.MuiTablePagination-selectLabel, .MuiTablePagination-displayedRows':
                      {
                        fontSize: '0.8rem',
                        color: theme.palette.text.secondary,
                      },
                    '.MuiTablePagination-actions': {
                      marginLeft: 1.5,
                    },
                  }}
                />
              </Box>
            )}

            {filteredParameters.length > 0 && (
              <Box
                sx={{
                  mt: 2,
                  display: 'flex',
                  flexDirection: { xs: 'column', sm: 'row' },
                  justifyContent: 'space-between',
                  alignItems: { xs: 'flex-start', sm: 'center' },
                  gap: 0.75,
                }}
              >
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                  <InfoIcon
                    fontSize="small"
                    sx={{ 
                      color: alpha(theme.palette.primary.main, 0.6),
                      fontSize: '0.9rem'
                    }}
                  />
                  <Typography
                    variant="body2"
                    sx={{
                      fontStyle: 'italic',
                      color: theme.palette.text.secondary,
                      fontSize: '0.75rem'
                    }}
                  >
                    Parameter values control core application behavior and
                    system configurations
                  </Typography>
                </Box>
                <Typography
                  variant="body2"
                  sx={{
                    color: theme.palette.primary.main,
                    backgroundColor: alpha(theme.palette.primary.main, 0.05),
                    py: 0.25,
                    px: 1.5,
                    borderRadius: 10,
                    fontWeight: 500,
                    fontSize: '0.75rem'
                  }}
                >
                  Showing {Math.min(rowsPerPage, filteredParameters.length)} of{' '}
                  {filteredParameters.length} parameters
                </Typography>
              </Box>
            )}
          </CardContent>
        </Card>
      </Box>
    </Box>
  )
}
