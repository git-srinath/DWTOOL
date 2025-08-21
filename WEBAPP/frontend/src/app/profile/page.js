'use client';

import { useState, useEffect } from 'react';
import { 
  Container, 
  Paper, 
  Typography, 
  Box, 
  Avatar, 
  Grid, 
  Divider, 
  Button, 
  IconButton, 
  Chip,
  Card,
  CardContent,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  useTheme,
  alpha,
  Stack,
  Tooltip,
  Alert,
  Snackbar,
  useMediaQuery
} from '@mui/material';
import { 
  Edit as EditIcon, 
  Security as SecurityIcon, 
  Logout as LogoutIcon,
  EmailOutlined as EmailIcon,
  PhoneOutlined as PhoneIcon,
  WorkOutline as WorkIcon,
  CalendarToday as CalendarIcon,
  AccountCircle as AccountIcon,
  Person as PersonIcon,
  SaveOutlined as SaveIcon,
  Cancel as CancelIcon
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { motion, AnimatePresence } from 'framer-motion';
import { useAuth } from '../context/AuthContext';
import { useRouter } from 'next/navigation';
import ProtectedRoute from '../components/ProtectedRoute';

const ProfileCard = styled(Card)(({ theme }) => ({
  borderRadius: theme.spacing(2),
  boxShadow: '0 8px 24px rgba(0, 0, 0, 0.06)',
  overflow: 'visible',
  position: 'relative',
  border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}`,
  maxWidth: '100%',
  transition: 'transform 0.3s ease, box-shadow 0.3s ease',
  '&:hover': {
    boxShadow: '0 12px 28px rgba(0, 0, 0, 0.08)',
  }
}));

const ProfileAvatar = styled(Avatar)(({ theme }) => ({
  width: 80,
  height: 80,
  border: `4px solid ${theme.palette.background.paper}`,
  boxShadow: '0 6px 16px rgba(0, 0, 0, 0.08)',
  fontSize: '2rem',
  backgroundColor: theme.palette.primary.main,
  position: 'absolute',
  top: -24,
  left: 24,
  transition: 'transform 0.3s ease',
  '&:hover': {
    transform: 'scale(1.05)',
  },
  [theme.breakpoints.down('md')]: {
    left: '50%',
    transform: 'translateX(-50%)',
    top: -40,
    width: 72,
    height: 72,
    '&:hover': {
      transform: 'translateX(-50%) scale(1.05)',
    },
  },
}));

const InfoItemCard = styled(Card)(({ theme }) => ({
  borderRadius: theme.spacing(2),
  boxShadow: '0 4px 12px rgba(0, 0, 0, 0.04)',
  height: '100%',
  border: `1px solid ${alpha(theme.palette.divider, 0.5)}`,
  transition: 'all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1)',
  '&:hover': {
    borderColor: alpha(theme.palette.primary.main, 0.3),
    transform: 'translateY(-4px)',
    boxShadow: '0 8px 20px rgba(0, 0, 0, 0.08), 0 4px 10px rgba(0, 0, 0, 0.06)',
  }
}));

const ProfileField = ({ label, value, icon }) => (
  <Box 
    sx={{ 
      display: 'flex', 
      alignItems: 'center', 
      mb: 1.5,
      transition: 'transform 0.2s ease',
      '&:hover': {
        transform: 'translateX(4px)'
      }
    }}
  >
    <Box 
      sx={{ 
        mr: 1.5, 
        color: 'primary.main',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        backgroundColor: (theme) => alpha(theme.palette.primary.main, 0.1),
        borderRadius: '50%',
        p: 0.75,
        width: 32,
        height: 32
      }}
    >
      {icon}
    </Box>
    <Box>
      <Typography variant="caption" color="textSecondary" fontWeight="500">
        {label}
      </Typography>
      <Typography variant="body2" fontWeight="medium">
        {value || 'Not specified'}
      </Typography>
    </Box>
  </Box>
);

const ProfilePage = () => {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  const isTablet = useMediaQuery(theme.breakpoints.down('md'));
  const { user, logout, updateUserProfile } = useAuth();
  const router = useRouter();
  const [profileData, setProfileData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [openPasswordDialog, setOpenPasswordDialog] = useState(false);
  const [openEditDialog, setOpenEditDialog] = useState(false);
  const [notification, setNotification] = useState({ open: false, message: '', severity: 'success' });
  const [editedData, setEditedData] = useState({
    first_name: '',
    last_name: '',
    email: '',
    phone: '',
    department: ''
  });
  const [passwordData, setPasswordData] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  });

  useEffect(() => {
    if (!user) return;

    // Initialize with user data from context
    setProfileData({
      ...user,
      is_active: true
    });
    setEditedData({
      first_name: user.first_name || '',
      last_name: user.last_name || '',
      email: user.email || '',
      phone: user.phone || '',
      department: user.department || ''
    });
    setLoading(false);
  }, [user]);

  const handleLogout = async () => {
    await logout();
    router.push('/auth/login');
  };

  const handleEditDialogOpen = () => {
    setOpenEditDialog(true);
  };

  const handleEditDialogClose = () => {
    setOpenEditDialog(false);
  };

  const handlePasswordDialogOpen = () => {
    setOpenPasswordDialog(true);
  };

  const handlePasswordDialogClose = () => {
    setOpenPasswordDialog(false);
    setPasswordData({
      currentPassword: '',
      newPassword: '',
      confirmPassword: ''
    });
  };

  const handleEditChange = (e) => {
    const { name, value } = e.target;
    setEditedData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handlePasswordChange = (e) => {
    const { name, value } = e.target;
    setPasswordData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSaveProfile = async () => {
    try {
      // TODO: Implement API call to save profile
      // For now, just update local state
      setProfileData(prev => ({
        ...prev,
        ...editedData
      }));
      
      updateUserProfile(editedData);
      
      setNotification({
        open: true,
        message: 'Profile updated successfully!',
        severity: 'success'
      });
      handleEditDialogClose();
    } catch (error) {
      setNotification({
        open: true,
        message: 'Failed to update profile.',
        severity: 'error'
      });
    }
  };

  const handleChangePassword = async () => {
    // Password validation
    if (passwordData.newPassword !== passwordData.confirmPassword) {
      setNotification({
        open: true,
        message: 'New passwords do not match.',
        severity: 'error'
      });
      return;
    }

    if (passwordData.newPassword.length < 8) {
      setNotification({
        open: true,
        message: 'Password must be at least 8 characters.',
        severity: 'error'
      });
      return;
    }

    try {
      // Implement API call to change password
      setNotification({
        open: true,
        message: 'Password changed successfully!',
        severity: 'success'
      });
      handlePasswordDialogClose();
    } catch (error) {
      setNotification({
        open: true,
        message: 'Failed to change password.',
        severity: 'error'
      });
    }
  };

  const handleCloseNotification = () => {
    setNotification(prev => ({
      ...prev,
      open: false
    }));
  };

  if (loading) {
    return (
      <Container sx={{ py: 4 }}>
        <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
          <Typography>Loading profile...</Typography>
        </Box>
      </Container>
    );
  }

  return (
    <ProtectedRoute>
      <AnimatePresence>
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.5 }}
        >
          <Container maxWidth="md" sx={{ py: { xs: 3, md: 4 } }}>
            <motion.div
              initial={{ y: 16 }}
              animate={{ y: 0 }}
              transition={{ 
                type: "spring", 
                stiffness: 300, 
                damping: 20 
              }}
            >
              <Box sx={{ position: 'relative', mt: { xs: 6, md: 3 }, mx: 'auto' }}>
                {/* Profile header */}
                <ProfileCard>
                  <Box
                    sx={{
                      height: { xs: 64, md: 90 },
                      background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${theme.palette.primary.dark} 100%)`,
                      borderTopLeftRadius: theme.spacing(2),
                      borderTopRightRadius: theme.spacing(2),
                      position: 'relative',
                      overflow: 'hidden',
                      '&::after': {
                        content: '""',
                        position: 'absolute',
                        bottom: 0,
                        right: 0,
                        width: '40%',
                        height: '100%',
                        background: 'rgba(255, 255, 255, 0.1)',
                        transform: 'skewX(-25deg) translateX(30%)',
                      }
                    }}
                  />
                  
                  <ProfileAvatar>
                    {profileData?.first_name?.charAt(0) || profileData?.username?.charAt(0) || 'U'}
                  </ProfileAvatar>

                  <Box sx={{ 
                    pt: { xs: 4, md: 1.5 }, 
                    pb: 2, 
                    px: { xs: 2, md: 3 }, 
                    mt: { xs: 1.5, md: 0 },
                    display: 'flex',
                    flexDirection: { xs: 'column', md: 'row' },
                    alignItems: { xs: 'center', md: 'flex-end' },
                    justifyContent: 'space-between'
                  }}>
                    <Box sx={{ 
                      textAlign: { xs: 'center', md: 'left' },
                      ml: { md: 10 }
                    }}>
                      <Typography variant={isTablet ? "subtitle1" : "h6"} fontWeight="bold" gutterBottom>
                        {`${profileData?.first_name || ''} ${profileData?.last_name || ''}`}
                        {!profileData?.first_name && !profileData?.last_name && profileData?.username}
                      </Typography>
                      
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: { xs: 'center', md: 'flex-start' }, flexWrap: 'wrap', gap: 0.75 }}>
                        <Chip 
                          label={profileData?.role || 'User'} 
                          color="primary" 
                          size="small"
                          sx={{ 
                            fontWeight: 500, 
                            height: 24,
                            fontSize: '0.75rem',
                            px: 0.75,
                            borderRadius: '20px',
                          }}
                        />
                        <Chip 
                          label={profileData?.is_active ? 'Active' : 'Inactive'} 
                          color={profileData?.is_active ? 'success' : 'error'} 
                          size="small"
                          variant="outlined"
                          sx={{ 
                            fontWeight: 500, 
                            height: 24,
                            fontSize: '0.75rem',
                            px: 0.75,
                            borderRadius: '20px', 
                          }}
                        />
                      </Box>
                    </Box>
                    
                    <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} sx={{ mt: { xs: 2, md: 0 }, width: { xs: '100%', sm: 'auto' } }}>
                      <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
                        <Button 
                          variant="outlined" 
                          startIcon={<SecurityIcon sx={{ fontSize: isMobile ? 16 : 18 }} />}
                          onClick={handlePasswordDialogOpen}
                          fullWidth={isMobile}
                          size={isMobile ? "small" : "medium"}
                          sx={{ 
                            borderRadius: 4,
                            borderWidth: 1.5,
                            fontSize: isMobile ? 12 : 13,
                            py: isMobile ? 0.5 : 0.75,
                            '&:hover': {
                              borderWidth: 1.5,
                              boxShadow: '0 3px 6px rgba(0, 0, 0, 0.07)',
                            }
                          }}
                        >
                          Change Password
                        </Button>
                      </motion.div>
                      <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
                        <Button 
                          variant="contained" 
                          color="primary"
                          startIcon={<LogoutIcon sx={{ fontSize: isMobile ? 16 : 18 }} />}
                          onClick={handleLogout}
                          fullWidth={isMobile}
                          size={isMobile ? "small" : "medium"}
                          sx={{ 
                            borderRadius: 4,
                            fontSize: isMobile ? 12 : 13,
                            py: isMobile ? 0.5 : 0.75,
                            boxShadow: '0 3px 8px rgba(0, 0, 0, 0.12)',
                            '&:hover': {
                              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.16)',
                            }
                          }}
                        >
                          Logout
                        </Button>
                      </motion.div>
                    </Stack>
                  </Box>
                </ProfileCard>

                {/* Profile details */}
                <Grid container spacing={2.5} sx={{ mt: 2.5 }}>
                  <Grid item xs={12} md={6}>
                    <motion.div 
                      initial={{ opacity: 0, x: -16 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: 0.2, duration: 0.4 }}
                    >
                      <InfoItemCard>
                        <CardContent sx={{ p: { xs: 2, md: 2.5 } }}>
                          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1.5 }}>
                            <Typography variant="subtitle1" fontWeight="bold" color="primary">
                              Account Information
                            </Typography>
                            <motion.div whileHover={{ rotate: 180 }} transition={{ duration: 0.3 }}>
                              <Tooltip title="Edit Profile">
                                <IconButton onClick={handleEditDialogOpen} color="primary" size="small" sx={{ 
                                  backgroundColor: alpha(theme.palette.primary.main, 0.1),
                                  width: 32,
                                  height: 32,
                                  '&:hover': { backgroundColor: alpha(theme.palette.primary.main, 0.2) }
                                }}>
                                  <EditIcon fontSize="small" sx={{ fontSize: 18 }} />
                                </IconButton>
                              </Tooltip>
                            </motion.div>
                          </Box>
                          <Divider sx={{ mb: 2 }} />
                          
                          <ProfileField 
                            label="Username" 
                            value={profileData?.username} 
                            icon={<AccountIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                          <ProfileField 
                            label="Email" 
                            value={profileData?.email} 
                            icon={<EmailIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                          <ProfileField 
                            label="Account Status" 
                            value={profileData?.is_active ? 'Active' : 'Inactive'} 
                            icon={<SecurityIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                          <ProfileField 
                            label="Member Since" 
                            value={
                              profileData?.created_at 
                                ? new Date(profileData.created_at).toLocaleDateString('en-US', {
                                    year: 'numeric',
                                    month: 'long',
                                    day: 'numeric'
                                  })
                                : undefined
                            } 
                            icon={<CalendarIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                        </CardContent>
                      </InfoItemCard>
                    </motion.div>
                  </Grid>
                  
                  <Grid item xs={12} md={6}>
                    <motion.div 
                      initial={{ opacity: 0, x: 16 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: 0.3, duration: 0.4 }}
                    >
                      <InfoItemCard>
                        <CardContent sx={{ p: { xs: 2, md: 2.5 } }}>
                          <Typography variant="subtitle1" fontWeight="bold" color="primary" gutterBottom>
                            Personal Information
                          </Typography>
                          <Divider sx={{ mb: 2 }} />
                          
                          <ProfileField 
                            label="First Name" 
                            value={profileData?.first_name} 
                            icon={<PersonIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                          <ProfileField 
                            label="Last Name" 
                            value={profileData?.last_name} 
                            icon={<PersonIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                          <ProfileField 
                            label="Phone" 
                            value={profileData?.phone} 
                            icon={<PhoneIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                          <ProfileField 
                            label="Department" 
                            value={profileData?.department} 
                            icon={<WorkIcon fontSize="small" sx={{ fontSize: 18 }} />}
                          />
                        </CardContent>
                      </InfoItemCard>
                    </motion.div>
                  </Grid>
                </Grid>
              </Box>
            </motion.div>
          </Container>
        </motion.div>
      </AnimatePresence>

      {/* Edit Profile Dialog */}
      <Dialog 
        open={openEditDialog} 
        onClose={handleEditDialogClose}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          elevation: 0,
          sx: {
            borderRadius: 2,
            border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}`,
          }
        }}
        TransitionComponent={motion.div}
      >
        <DialogTitle sx={{ 
          fontWeight: 600,
          borderBottom: `1px solid ${theme.palette.divider}`,
          pb: 1,
          fontSize: '1.1rem'
        }}>
          Edit Profile
        </DialogTitle>
        <DialogContent sx={{ pt: 2.5 }}>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6}>
              <TextField
                label="First Name"
                name="first_name"
                value={editedData.first_name}
                onChange={handleEditChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                label="Last Name"
                name="last_name"
                value={editedData.last_name}
                onChange={handleEditChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                label="Email"
                name="email"
                value={editedData.email}
                onChange={handleEditChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                label="Phone"
                name="phone"
                value={editedData.phone}
                onChange={handleEditChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                label="Department"
                name="department"
                value={editedData.department}
                onChange={handleEditChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ px: 3, py: 1.5, borderTop: `1px solid ${theme.palette.divider}` }}>
          <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
            <Button 
              onClick={handleEditDialogClose}
              startIcon={<CancelIcon sx={{ fontSize: 18 }} />}
              variant="outlined"
              size="small"
              sx={{ 
                borderRadius: 4,
                borderWidth: 1.5,
                fontSize: 13,
                '&:hover': {
                  borderWidth: 1.5,
                }
              }}
            >
              Cancel
            </Button>
          </motion.div>
          <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
            <Button 
              onClick={handleSaveProfile} 
              color="primary" 
              variant="contained"
              startIcon={<SaveIcon sx={{ fontSize: 18 }} />}
              size="small"
              sx={{ 
                borderRadius: 4,
                fontSize: 13,
                boxShadow: theme.shadows[1],
                '&:hover': {
                  boxShadow: theme.shadows[3],
                }
              }}
            >
              Save Changes
            </Button>
          </motion.div>
        </DialogActions>
      </Dialog>

      {/* Change Password Dialog */}
      <Dialog 
        open={openPasswordDialog} 
        onClose={handlePasswordDialogClose}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          elevation: 0,
          sx: {
            borderRadius: 2,
            border: `1px solid ${alpha(theme.palette.primary.main, 0.1)}`,
          }
        }}
        TransitionComponent={motion.div}
      >
        <DialogTitle sx={{ 
          fontWeight: 600,
          borderBottom: `1px solid ${theme.palette.divider}`,
          pb: 1,
          fontSize: '1.1rem'
        }}>
          Change Password
        </DialogTitle>
        <DialogContent sx={{ pt: 2.5 }}>
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <TextField
                label="Current Password"
                name="currentPassword"
                type="password"
                value={passwordData.currentPassword}
                onChange={handlePasswordChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                label="New Password"
                name="newPassword"
                type="password"
                value={passwordData.newPassword}
                onChange={handlePasswordChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                label="Confirm New Password"
                name="confirmPassword"
                type="password"
                value={passwordData.confirmPassword}
                onChange={handlePasswordChange}
                fullWidth
                variant="outlined"
                size="small"
                InputProps={{ 
                  sx: { borderRadius: 1.5 } 
                }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions sx={{ px: 3, py: 1.5, borderTop: `1px solid ${theme.palette.divider}` }}>
          <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
            <Button 
              onClick={handlePasswordDialogClose}
              startIcon={<CancelIcon sx={{ fontSize: 18 }} />}
              variant="outlined"
              size="small"
              sx={{ 
                borderRadius: 4,
                borderWidth: 1.5,
                fontSize: 13,
                '&:hover': {
                  borderWidth: 1.5,
                }
              }}
            >
              Cancel
            </Button>
          </motion.div>
          <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
            <Button 
              onClick={handleChangePassword} 
              color="primary" 
              variant="contained"
              startIcon={<SecurityIcon sx={{ fontSize: 18 }} />}
              size="small"
              sx={{ 
                borderRadius: 4,
                fontSize: 13,
                boxShadow: theme.shadows[1],
                '&:hover': {
                  boxShadow: theme.shadows[3],
                }
              }}
            >
              Update Password
            </Button>
          </motion.div>
        </DialogActions>
      </Dialog>

      {/* Notifications */}
      <Snackbar 
        open={notification.open} 
        autoHideDuration={6000} 
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert 
          onClose={handleCloseNotification} 
          severity={notification.severity} 
          variant="filled"
          sx={{ 
            width: '100%',
            borderRadius: 1.5,
            boxShadow: '0 4px 16px rgba(0,0,0,0.12)'
          }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </ProtectedRoute>
  );
};

export default ProfilePage; 