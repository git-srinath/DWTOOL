import React from 'react';
import { 
  Box, 
  Typography, 
  Divider, 
  Avatar, 
  useTheme, 
  Link, 
  Stack, 
  IconButton, 
  Grid, 
  Paper, 
  Tooltip, 
  Container 
} from '@mui/material';
import { alpha } from '@mui/material/styles';
import LanguageIcon from '@mui/icons-material/Language';
import LinkedInIcon from '@mui/icons-material/LinkedIn';
import TwitterIcon from '@mui/icons-material/Twitter';
import GitHubIcon from '@mui/icons-material/GitHub';
import EmailIcon from '@mui/icons-material/Email';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import BusinessIcon from '@mui/icons-material/Business';
import VerifiedIcon from '@mui/icons-material/Verified';
import ConnectWithoutContactIcon from '@mui/icons-material/ConnectWithoutContact';

const AboutTabContent = () => {
  const theme = useTheme();

  return (
    <Box sx={{
      width: '100%',
      bgcolor: theme.palette.background.default,
      py: 2,
    }}>
      {/* Hero Section */}
      <Paper
        elevation={0}
        sx={{
          borderRadius: 0,
          background: `linear-gradient(120deg, ${alpha(theme.palette.primary.main, 0.08)} 0%, ${alpha(theme.palette.primary.dark, 0.12)} 100%)`,
          py: 4,
          px: { xs: 2, sm: 4, md: 6 },
          mb: 4,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
          gap: 3,
        }}
      >
        <Stack direction="row" spacing={3} alignItems="center">
          <Avatar
            sx={{
              bgcolor: theme.palette.primary.main,
              width: { xs: 50, sm: 60 },
              height: { xs: 50, sm: 60 },
              boxShadow: theme.shadows[3],
              fontSize: '1.8rem',
            }}
          >
            A
          </Avatar>
          <Stack>
            <Typography 
              variant="h4" 
              component="h1" 
              fontWeight="700" 
              color="primary.dark"
              sx={{ fontSize: { xs: '1.5rem', sm: '2rem' } }}
            >
              Ahana DW Tool
            </Typography>
            <Typography 
              variant="subtitle1" 
              color="text.secondary"
              sx={{ fontSize: { xs: '0.875rem', sm: '1rem' } }}
            >
              Data Warehouse Management System
            </Typography>
          </Stack>
        </Stack>
        
        <Box sx={{ 
          bgcolor: alpha(theme.palette.success.main, 0.1),
          color: theme.palette.success.dark,
          borderRadius: 2,
          px: 2,
          py: 0.5,
          display: 'flex',
          alignItems: 'center',
        }}>
          <VerifiedIcon fontSize="small" sx={{ mr: 1 }} />
          <Typography variant="body2" fontWeight="medium">v1.0.0 • Up to date</Typography>
        </Box>
      </Paper>

      {/* Main Content Grid */}
      <Grid container spacing={3} sx={{ px: { xs: 2, sm: 3 } }}>
        {/* Company Card */}
        <Grid item xs={12} md={6} lg={4}>
          <Paper 
            elevation={0} 
            sx={{ 
              borderRadius: 2,
              height: '100%',
              border: `1px solid ${alpha(theme.palette.divider, 0.5)}`,
              p: { xs: 2, sm: 3 },
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                boxShadow: theme.shadows[2],
                borderColor: theme.palette.primary.light,
              }
            }}
          >
            <Stack spacing={2} height="100%">
              <Stack direction="row" spacing={1.5} alignItems="center">
                <Box sx={{ 
                  p: 1, 
                  borderRadius: '50%', 
                  bgcolor: alpha(theme.palette.primary.main, 0.1),
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center'
                }}>
                  <BusinessIcon color="primary" fontSize="small" />
                </Box>
                <Typography variant="h6" fontWeight="600" color="primary.main">
                  About the Company
                </Typography>
              </Stack>
              
              <Box sx={{ py: 1 }}>
                <Typography variant="body1" fontWeight="medium" gutterBottom>
                  Ahana Systems & Solutions Pvt Ltd
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                  "Creating Possibilities"
                </Typography>
              </Box>

              <Divider sx={{ my: 1 }} />
              
              <Stack spacing={1.5} sx={{ mt: 'auto' }}>
                <Stack direction="row" spacing={1.5} alignItems="center">
                  <LanguageIcon fontSize="small" color="action" />
                  <Link
                    href="https://www.ahanait.com"
                    target="_blank"
                    rel="noopener noreferrer"
                    variant="body2"
                    underline="hover"
                  >
                    www.ahanait.com
                  </Link>
                </Stack>
                <Stack direction="row" spacing={1.5} alignItems="center">
                  <EmailIcon fontSize="small" color="action" />
                  <Link
                    href="mailto:info@ahanait.co.in"
                    variant="body2"
                    underline="hover"
                  >
                    info@ahanait.co.in
                  </Link>
                </Stack>
              </Stack>
            </Stack>
          </Paper>
        </Grid>

        {/* Application Info Card */}
        <Grid item xs={12} md={6} lg={4}>
          <Paper 
            elevation={0} 
            sx={{ 
              borderRadius: 2,
              height: '100%',
              border: `1px solid ${alpha(theme.palette.divider, 0.5)}`,
              p: { xs: 2, sm: 3 },
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                boxShadow: theme.shadows[2],
                borderColor: theme.palette.primary.light,
              }
            }}
          >
            <Stack spacing={2} height="100%">
              <Stack direction="row" spacing={1.5} alignItems="center">
                <Box sx={{ 
                  p: 1, 
                  borderRadius: '50%', 
                  bgcolor: alpha(theme.palette.primary.main, 0.1),
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center'
                }}>
                  <InfoOutlinedIcon color="primary" fontSize="small" />
                </Box>
                <Typography variant="h6" fontWeight="600" color="primary.main">
                  Application Info
                </Typography>
              </Stack>
              
              <Box sx={{ py: 1 }}>
                <Grid container spacing={2}>
                  <Grid item xs={4}>
                    <Typography variant="body2" color="text.secondary">Version</Typography>
                    <Typography variant="body1" fontWeight="500">1.0.0</Typography>
                  </Grid>
                  <Grid item xs={8}>
                    <Typography variant="body2" color="text.secondary">Status</Typography>
                    <Typography variant="body1" fontWeight="500" color="success.main" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      <VerifiedIcon fontSize="small" /> Up to date
                    </Typography>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="body2" color="text.secondary">Last Checked</Typography>
                    <Typography variant="body1">{new Date().toLocaleDateString()}</Typography>
                  </Grid>
                </Grid>
              </Box>

              <Box sx={{ mt: 'auto', pt: 1 }}>
                <Typography variant="body2" color="text.secondary">
                  © {new Date().getFullYear()} Ahana Systems & Solutions Pvt Ltd
                </Typography>
              </Box>
            </Stack>
          </Paper>
        </Grid>

        {/* Connect With Us Card */}
        <Grid item xs={12} md={12} lg={4}>
          <Paper 
            elevation={0} 
            sx={{ 
              borderRadius: 2,
              height: '100%',
              border: `1px solid ${alpha(theme.palette.divider, 0.5)}`,
              p: { xs: 2, sm: 3 },
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                boxShadow: theme.shadows[2],
                borderColor: theme.palette.primary.light,
              }
            }}
          >
            <Stack spacing={2} height="100%">
              <Stack direction="row" spacing={1.5} alignItems="center">
                <Box sx={{ 
                  p: 1, 
                  borderRadius: '50%', 
                  bgcolor: alpha(theme.palette.primary.main, 0.1),
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center'
                }}>
                  <ConnectWithoutContactIcon color="primary" fontSize="small" />
                </Box>
                <Typography variant="h6" fontWeight="600" color="primary.main">
                  Connect With Us
                </Typography>
              </Stack>
              
              <Typography variant="body2" color="text.secondary">
                Follow us on our social channels for updates and news.
              </Typography>

              <Box sx={{ 
                display: 'flex', 
                gap: 1.5, 
                flexWrap: 'wrap', 
                justifyContent: 'center',
                mt: 2 
              }}>
                <Tooltip title="Website">
                  <IconButton
                    component={Link}
                    href="https://www.ahanait.com"
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label="Visit Ahana Website"
                    sx={{
                      color: theme.palette.primary.main,
                      bgcolor: alpha(theme.palette.primary.main, 0.1),
                      '&:hover': { 
                        bgcolor: alpha(theme.palette.primary.main, 0.2),
                        transform: 'translateY(-2px)'
                      },
                      transition: 'all 0.2s',
                    }}
                  >
                    <LanguageIcon />
                  </IconButton>
                </Tooltip>

                <Tooltip title="Email">
                  <IconButton
                    component={Link}
                    href="mailto:info@ahanait.co.in"
                    aria-label="Send Email to Ahana"
                    sx={{
                      color: '#EA4335',
                      bgcolor: alpha('#EA4335', 0.1),
                      '&:hover': { 
                        bgcolor: alpha('#EA4335', 0.2),
                        transform: 'translateY(-2px)'
                      },
                      transition: 'all 0.2s',
                    }}
                  >
                    <EmailIcon />
                  </IconButton>
                </Tooltip>

                <Tooltip title="LinkedIn">
                  <IconButton
                    component={Link}
                    href="https://www.linkedin.com/company/ahana-systems-solutions/"
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label="Ahana on LinkedIn"
                    sx={{
                      color: '#0077B5',
                      bgcolor: alpha('#0077B5', 0.1),
                      '&:hover': { 
                        bgcolor: alpha('#0077B5', 0.2),
                        transform: 'translateY(-2px)'
                      },
                      transition: 'all 0.2s',
                    }}
                  >
                    <LinkedInIcon />
                  </IconButton>
                </Tooltip>

                <Tooltip title="Twitter">
                  <IconButton
                    component={Link}
                    href="https://twitter.com/ahana_it"
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label="Ahana on Twitter"
                    sx={{
                      color: '#1DA1F2',
                      bgcolor: alpha('#1DA1F2', 0.1),
                      '&:hover': { 
                        bgcolor: alpha('#1DA1F2', 0.2),
                        transform: 'translateY(-2px)'
                      },
                      transition: 'all 0.2s',
                    }}
                  >
                    <TwitterIcon />
                  </IconButton>
                </Tooltip>

                <Tooltip title="GitHub">
                  <IconButton
                    component={Link}
                    href="https://github.com/ahana-systems"
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label="Ahana on GitHub"
                    sx={{
                      color: theme.palette.mode === 'dark' ? '#fff' : '#24292e',
                      bgcolor: alpha(theme.palette.mode === 'dark' ? '#fff' : '#24292e', 0.1),
                      '&:hover': { 
                        bgcolor: alpha(theme.palette.mode === 'dark' ? '#fff' : '#24292e', 0.2),
                        transform: 'translateY(-2px)'
                      },
                      transition: 'all 0.2s',
                    }}
                  >
                    <GitHubIcon />
                  </IconButton>
                </Tooltip>
              </Box>
              
              <Typography variant="caption" color="text.secondary" align="center" sx={{ mt: 'auto', pt: 2 }}>
                All rights reserved • Ahana DW Tool v1.0.0
              </Typography>
            </Stack>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default AboutTabContent;