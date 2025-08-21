'use client'

import { useState, useRef, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import {
  Typography,
  Box,
  Alert,
  TextField,
  Checkbox,
  FormControlLabel,
  IconButton,
  InputAdornment,
} from '@mui/material'
import { motion, AnimatePresence } from 'framer-motion'
import { styled } from '@mui/material/styles'
import Link from 'next/link'
import { useAuth } from '../../context/AuthContext'
import LockOutlinedIcon from '@mui/icons-material/LockOutlined'
import AccountCircleOutlinedIcon from '@mui/icons-material/AccountCircleOutlined'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
import StorageIcon from '@mui/icons-material/Storage'
import CloudIcon from '@mui/icons-material/Cloud'
import StorageOutlinedIcon from '@mui/icons-material/StorageOutlined'
import DataUsageIcon from '@mui/icons-material/DataUsage'
import { ThemeProvider, createTheme } from '@mui/material/styles'
import ReCAPTCHA from 'react-google-recaptcha'
import { RECAPTCHA_SITE_KEY, ENABLE_RECAPTCHA } from '../../config'
import React from 'react'

// Create a custom theme with data warehouse colors
const dwTheme = createTheme({
  palette: {
    primary: {
      main: '#1A73E8',
      light: '#4285F4',
      dark: '#0D47A1',
    },
    secondary: {
      main: '#34A853',
      light: '#4CD964',
      dark: '#1E8E3E',
    },
    background: {
      default: '#F8F9FA',
      paper: '#FFFFFF',
    },
    error: {
      main: '#EA4335',
    },
  },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
  },
  shape: {
    borderRadius: 12,
  },
  shadows: [
    'none',
    '0px 2px 8px rgba(0, 0, 0, 0.06)',
    '0px 4px 16px rgba(0, 0, 0, 0.08)',
    '0px 8px 24px rgba(0, 0, 0, 0.1)',
    // Additional shadows...
    ...Array(21).fill('none'),
  ],
});

// Main container with background animation
const LoginContainer = styled(Box)`
  display: flex;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
  position: relative;
  background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);

  @keyframes float {
    0% { transform: translatey(0px); }
    50% { transform: translatey(-20px); }
    100% { transform: translatey(0px); }
  }

  @keyframes pulse {
    0% { opacity: 0.5; }
    50% { opacity: 1; }
    100% { opacity: 0.5; }
  }
`;

// Left panel with dynamic data visualization elements
const LeftPanel = styled(Box)`
  flex: 1.2;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #1A73E8 0%, #0D47A1 100%);
  overflow: hidden;

  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: radial-gradient(circle at 30% 50%, rgba(66, 133, 244, 0.6) 0%, rgba(26, 115, 232, 0) 70%),
               radial-gradient(circle at 70% 20%, rgba(13, 71, 161, 0.4) 0%, rgba(13, 71, 161, 0) 70%);
    z-index: 1;
  }

  @media (max-width: 960px) {
    display: none;
  }
`;

// Right panel containing the login form with glass morphism
const RightPanel = styled(Box)`
  flex: 0.8;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  padding: 2rem;
  position: relative;
  background: rgba(255, 255, 255, 0.85);
  backdrop-filter: blur(10px);
  z-index: 2;

  @media (max-width: 960px) {
    flex: none;
    width: 100%;
    background: linear-gradient(135deg, #1A73E8 0%, #0D47A1 100%);
  }
`;

// Form container with glass morphism effect
const FormContainer = styled(Box)`
  width: 100%;
  max-width: 380px;
  padding: 2.5rem;
  border-radius: 1.25rem;
  background: rgba(255, 255, 255, 0.8);
  backdrop-filter: blur(10px);
  box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
  transition: all 0.4s ease;

  &:hover {
    box-shadow: 0 15px 50px rgba(0, 0, 0, 0.15);
    transform: translateY(-5px);
  }

  @media (max-width: 960px) {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(20px);
  }
`;

// Enhanced login button with advanced animation
const LoginButton = styled(motion.button)`
  width: 100%;
  padding: 0.875rem;
  margin-top: 1.5rem;
  border: none;
  border-radius: 0.75rem;
  background: linear-gradient(90deg, #1A73E8 0%, #0D47A1 100%);
  color: white;
  font-size: 1rem;
  font-weight: 600;
  cursor: pointer;
  position: relative;
  overflow: hidden;
  display: flex;
  justify-content: center;
  align-items: center;
  transition: all 0.3s ease;

  &:hover {
    background: linear-gradient(90deg, #4285F4 0%, #1A73E8 100%);
  }

  &::after {
    content: '';
    position: absolute;
    top: 50%;
    left: 50%;
    width: 120%;
    height: 0px;
    background: rgba(255, 255, 255, 0.2);
    border-radius: 100%;
    transform: translate(-50%, -50%) scale(0);
    opacity: 0;
    z-index: 0;
    transition: transform 0.5s cubic-bezier(0.175, 0.885, 0.32, 1.275), opacity 0.5s ease;
  }

  &:hover::after {
    opacity: 1;
    height: 400%;
    transform: translate(-50%, -50%) scale(1);
  }

  span {
    z-index: 1;
    position: relative;
  }
`;

// Improved styled text field with better focus states
const StyledTextField = styled(TextField)`
  margin-bottom: 1.25rem;

  .MuiOutlinedInput-root {
    border-radius: 0.75rem;
    transition: all 0.3s ease;
    font-size: 0.9375rem;
    background: rgba(255, 255, 255, 0.8);

    &.Mui-focused {
      background: rgba(255, 255, 255, 0.95);
      box-shadow: 0 4px 12px rgba(26, 115, 232, 0.15);
      
      .MuiOutlinedInput-notchedOutline {
        border-color: #1A73E8;
        border-width: 2px;
      }
    }

    &:hover .MuiOutlinedInput-notchedOutline {
      border-color: #4285F4;
    }
  }

  .MuiInputLabel-root {
    font-size: 0.9375rem;

    &.Mui-focused {
      color: #1A73E8;
    }
  }

  .MuiInputAdornment-root {
    color: #1A73E8;
  }
`;

// Logo with more dynamic animation
const Logo = styled(motion.div)`
  margin-bottom: 1.5rem;
  font-size: 2rem;
  font-weight: 800;
  display: flex;
  align-items: center;
  justify-content: center;

  span {
    display: inline-block;
    margin-left: 0.5rem;
    background: linear-gradient(90deg, #1A73E8, #0D47A1);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
`;

// Floating database icon component
const FloatingIcon = ({ icon: Icon, style, delay, duration }) => {
  if (!Icon) return null;
  
  return (
    <motion.div
      initial={{ y: 0, opacity: 0.7 }}
      animate={{ 
        y: [-20, 20], 
        opacity: [0.7, 1, 0.7],
      }}
      transition={{ 
        y: { 
          repeat: Infinity, 
          repeatType: "reverse", 
          duration, 
          delay 
        },
        opacity: {
          repeat: Infinity,
          repeatType: "reverse",
          duration: duration * 1.5,
          delay
        }
      }}
      style={{
        position: 'absolute',
        color: 'rgba(255, 255, 255, 0.25)',
        zIndex: 0,
        ...style
      }}
    >
      {React.createElement(Icon, { sx: { fontSize: style.size || '3rem' } })}
    </motion.div>
  );
};

// Particle effect for the background
const Particle = ({ delay, x, y, size }) => (
  <motion.div
    initial={{ opacity: 0, scale: 0 }}
    animate={{ 
      opacity: [0, 0.5, 0],
      scale: [0, 1, 0],
      x: x,
      y: y
    }}
    transition={{ 
      repeat: Infinity, 
      duration: 4 + Math.random() * 2,
      delay: delay,
      ease: "easeInOut"
    }}
    style={{
      position: 'absolute',
      width: size,
      height: size,
      borderRadius: '50%',
      background: 'white',
      zIndex: 0,
      filter: 'blur(1px)'
    }}
  />
);

// Login page component
const LoginPage = () => {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [rememberMe, setRememberMe] = useState(false)
  const [error, setError] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [captchaVerified, setCaptchaVerified] = useState(false)
  const [recaptchaToken, setRecaptchaToken] = useState('')
  const [showElements, setShowElements] = useState(false)
  const recaptchaRef = useRef(null)
  const router = useRouter()
  const { login } = useAuth()

  // Check if using test key (for development warning)
  const isUsingTestKey = RECAPTCHA_SITE_KEY === '6LeIxAcTAAAAAJcZVRqyHh71UMIEGNQ_MXjiZKhI';

  // Delayed animation start
  useEffect(() => {
    const timer = setTimeout(() => {
      setShowElements(true);
    }, 300);
    return () => clearTimeout(timer);
  }, []);

  const handleClickShowPassword = () => {
    setShowPassword(!showPassword)
  }

  const handleMouseDownPassword = (event) => {
    event.preventDefault()
  }

  const handleCaptchaChange = (value) => {
    setCaptchaVerified(!!value)
    setRecaptchaToken(value || '')
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')

    if (!username || !password) {
      setError('Please enter both username and password')
      return
    }

    if (ENABLE_RECAPTCHA && RECAPTCHA_SITE_KEY && !captchaVerified) {
      setError('Please verify that you are not a robot')
      return
    }

    setIsLoading(true)
    try {
      const result = await login(
        username, 
        password, 
        (ENABLE_RECAPTCHA && RECAPTCHA_SITE_KEY) ? recaptchaToken : undefined
      )
      if (result.success) {
        router.push('/home')
      } else {
        setError(result.error)
        if (ENABLE_RECAPTCHA && recaptchaRef.current) {
          recaptchaRef.current.reset()
          setCaptchaVerified(false)
          setRecaptchaToken('')
        }
      }
    } finally {
      setIsLoading(false)
    }
  }

  // Enhanced animation variants
  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1,
        delayChildren: 0.3,
      },
    },
  }

  const itemVariants = {
    hidden: { y: 20, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
      transition: { type: 'spring', stiffness: 300, damping: 24 },
    },
  }

  // Generate random particles for visual effect
  const renderParticles = () => {
    const particles = [];
    for (let i = 0; i < 20; i++) {
      particles.push(
        <Particle 
          key={i}
          delay={i * 0.2}
          x={[Math.random() * 200 - 100, Math.random() * 200 - 100]}
          y={[Math.random() * 200 - 100, Math.random() * 200 - 100]}
          size={Math.random() * 6 + 2 + 'px'}
        />
      );
    }
    return particles;
  };

  return (
    <ThemeProvider theme={dwTheme}>
      <LoginContainer>
        {/* Left panel with data visualization elements */}
        <LeftPanel>
          {/* Floating icons representing data warehousing */}
          {StorageOutlinedIcon && <FloatingIcon icon={StorageOutlinedIcon} style={{ top: '15%', left: '20%', size: '4rem' }} delay={0} duration={3} />}
          {StorageIcon && <FloatingIcon icon={StorageIcon} style={{ top: '30%', right: '25%', size: '3.5rem' }} delay={0.5} duration={4} />}
          {CloudIcon && <FloatingIcon icon={CloudIcon} style={{ bottom: '20%', left: '30%', size: '3rem' }} delay={1.5} duration={3.5} />}
          {DataUsageIcon && <FloatingIcon icon={DataUsageIcon} style={{ bottom: '40%', right: '15%', size: '2.5rem' }} delay={2} duration={4.5} />}
          
          {/* Background particles */}
          {renderParticles()}

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, delay: 0.5 }}
            style={{
              zIndex: 2,
              maxWidth: '80%',
              textAlign: 'center',
              color: 'white',
              position: 'relative',
            }}
          >
            <Typography
              variant="h2"
              component="h1"
              sx={{
                fontWeight: 800,
                marginBottom: 3,
                textShadow: '0 2px 15px rgba(0, 0, 0, 0.2)',
                letterSpacing: '-0.5px',
                fontSize: { xs: '2rem', sm: '2.5rem', md: '3rem' },
                lineHeight: 1.2,
              }}
            >
              Data Warehouse Tool
            </Typography>
            <Typography
              variant="h5"
              sx={{
                fontWeight: 400,
                marginBottom: 4,
                opacity: 0.9,
                textShadow: '0 2px 10px rgba(0, 0, 0, 0.15)',
                maxWidth: '80%',
                mx: 'auto',
                lineHeight: 1.6,
                fontSize: { xs: '1rem', sm: '1.15rem', md: '1.25rem' },
              }}
            >
              Powerful insights from your data, simplified
            </Typography>

            <Box
              sx={{
                display: 'flex',
                justifyContent: 'center',
                gap: { xs: 1.5, sm: 2, md: 3 },
                flexWrap: 'wrap',
              }}
            >
              {[
                { title: '100+', subtitle: 'Data Connectors', delay: 0 },
                { title: 'Fast', subtitle: 'Data Processing', delay: 0.1 },
                { title: 'Secure', subtitle: 'Enterprise Ready', delay: 0.2 },
                { title: 'Real-time', subtitle: 'Visualizations', delay: 0.3 }
              ].map((item, index) => (
                <motion.div
                  key={index}
                  initial={{ opacity: 0, scale: 0.8, y: 20 }}
                  animate={{ opacity: 1, scale: 1, y: 0 }}
                  transition={{ 
                    type: 'spring', 
                    stiffness: 300, 
                    damping: 24, 
                    delay: 0.8 + item.delay 
                  }}
                  whileHover={{ 
                    scale: 1.05, 
                    y: -8,
                    boxShadow: '0 12px 30px rgba(0, 0, 0, 0.15)' 
                  }}
                >
                  <Box
                    sx={{
                      background: 'rgba(255, 255, 255, 0.1)',
                      backdropFilter: 'blur(10px)',
                      padding: { xs: 2, sm: 2.5 },
                      borderRadius: 3,
                      textAlign: 'center',
                      width: { xs: '130px', sm: '150px' },
                      height: { xs: '100px', sm: '110px' },
                      display: 'flex',
                      flexDirection: 'column',
                      justifyContent: 'center',
                      border: '1px solid rgba(255, 255, 255, 0.2)',
                      position: 'relative',
                      overflow: 'hidden',
                    }}
                  >
                    {/* Background glow effect */}
                    <Box 
                      sx={{
                        position: 'absolute',
                        width: '100%',
                        height: '100%',
                        background: 'radial-gradient(circle at center, rgba(255,255,255,0.2) 0%, rgba(255,255,255,0) 70%)',
                        top: 0,
                        left: 0,
                      }}
                    />
                    
                    <Typography 
                      variant="h4" 
                      sx={{ 
                        fontWeight: 700, 
                        mb: 1, 
                        fontSize: { xs: '1.5rem', sm: '1.75rem' },
                        position: 'relative',
                        zIndex: 1,
                      }}
                    >
                      {item.title}
                    </Typography>
                    <Typography 
                      variant="body1" 
                      sx={{ 
                        fontSize: { xs: '0.85rem', sm: '0.9rem' },
                        position: 'relative',
                        zIndex: 1,
                        opacity: 0.9,
                      }}
                    >
                      {item.subtitle}
                    </Typography>
                  </Box>
                </motion.div>
              ))}
            </Box>
          </motion.div>
        </LeftPanel>

        {/* Right panel with login form */}
        <RightPanel>
          <AnimatePresence>
            {showElements && (
              <motion.div
                variants={containerVariants}
                initial="hidden"
                animate="visible"
                style={{ width: '100%', maxWidth: '380px', position: 'relative', zIndex: 3 }}
              >
                <FormContainer
                  as={motion.form}
                  onSubmit={handleSubmit}
                  variants={itemVariants}
                >
                  <Logo
                    initial={{ scale: 0 }}
                    animate={{ scale: 1 }}
                    transition={{
                      type: 'spring',
                      stiffness: 400,
                      damping: 20,
                      delay: 0.4,
                    }}
                  >
                    <StorageOutlinedIcon sx={{ 
                      fontSize: '2.25rem', 
                      color: '#1A73E8',
                      marginRight: '0.5rem',
                      filter: 'drop-shadow(0px 2px 4px rgba(26, 115, 232, 0.3))'
                    }} />
                    DW<span>Tool</span>
                  </Logo>

                  <motion.div variants={itemVariants}>
                    <Typography
                      variant="h5"
                      align="center"
                      gutterBottom
                      sx={{
                        fontWeight: 700,
                        mb: 3,
                        fontSize: '1.375rem',
                        background: 'linear-gradient(90deg, #1A73E8, #0D47A1)',
                        backgroundClip: 'text',
                        WebkitBackgroundClip: 'text',
                        WebkitTextFillColor: 'transparent',
                        letterSpacing: '0.5px',
                      }}
                    >
                      Sign In to Your Account
                    </Typography>
                  </motion.div>

                  <AnimatePresence>
                    {error && (
                      <motion.div
                        initial={{ opacity: 0, y: -10, height: 0 }}
                        animate={{ opacity: 1, y: 0, height: 'auto' }}
                        exit={{ opacity: 0, y: -10, height: 0 }}
                        transition={{ duration: 0.3 }}
                      >
                        <Alert
                          severity="error"
                          sx={{ 
                            mb: 2.5, 
                            borderRadius: '0.75rem',
                            boxShadow: '0 4px 12px rgba(234, 67, 53, 0.15)'
                          }}
                        >
                          {error}
                        </Alert>
                      </motion.div>
                    )}
                  </AnimatePresence>

                  <motion.div variants={itemVariants}>
                    <StyledTextField
                      label="Username"
                      variant="outlined"
                      fullWidth
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                      required
                      InputProps={{
                        startAdornment: (
                          <AccountCircleOutlinedIcon sx={{ mr: 1, color: '#1A73E8' }} />
                        ),
                      }}
                    />
                  </motion.div>

                  <motion.div variants={itemVariants}>
                    <StyledTextField
                      label="Password"
                      type={showPassword ? 'text' : 'password'}
                      variant="outlined"
                      fullWidth
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      required
                      InputProps={{
                        startAdornment: (
                          <LockOutlinedIcon sx={{ mr: 1, color: '#1A73E8' }} />
                        ),
                        endAdornment: (
                          <InputAdornment position="end">
                            <IconButton
                              aria-label="toggle password visibility"
                              onClick={handleClickShowPassword}
                              onMouseDown={handleMouseDownPassword}
                              edge="end"
                              size="small"
                              sx={{ color: '#1A73E8' }}
                            >
                              {showPassword ? <VisibilityOffIcon /> : <VisibilityIcon />}
                            </IconButton>
                          </InputAdornment>
                        ),
                      }}
                    />
                  </motion.div>

                  <motion.div
                    variants={itemVariants}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      marginBottom: '1rem',
                    }}
                  >
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={rememberMe}
                          onChange={(e) => setRememberMe(e.target.checked)}
                          sx={{
                            color: '#adb5bd',
                            '&.Mui-checked': { color: '#1A73E8' },
                            '& .MuiSvgIcon-root': { fontSize: '1.125rem' },
                          }}
                          size="small"
                        />
                      }
                      label={<Typography variant="body2" sx={{ fontSize: '0.875rem' }}>Remember me</Typography>}
                    />
                    <Link
                      href="/auth/forgot-password"
                      style={{
                        color: '#1A73E8',
                        textDecoration: 'none',
                        fontWeight: 600,
                        fontSize: '0.875rem',
                        transition: 'all 0.2s ease',
                      }}
                    >
                      Forgot Password?
                    </Link>
                  </motion.div>

                  {/* ReCAPTCHA Component */}
                  <motion.div 
                    variants={itemVariants}
                    style={{ marginBottom: '1.25rem', display: 'flex', flexDirection: 'column', alignItems: 'center' }}
                  >
                    {ENABLE_RECAPTCHA ? (
                      RECAPTCHA_SITE_KEY ? (
                        <>
                          <ReCAPTCHA
                            ref={recaptchaRef}
                            sitekey={RECAPTCHA_SITE_KEY}
                            onChange={handleCaptchaChange}
                            size="normal"
                            theme="light"
                          />
                          {isUsingTestKey && (
                            <Typography 
                              variant="caption" 
                              color="warning.main" 
                              sx={{ mt: 1, fontSize: '0.75rem' }}
                            >
                              Using test reCAPTCHA key (for development only)
                            </Typography>
                          )}
                        </>
                      ) : (
                        <Alert 
                          severity="warning" 
                          sx={{ mb: 1, width: '100%', borderRadius: '0.75rem' }}
                        >
                          ReCAPTCHA site key is missing. Please configure it.
                        </Alert>
                      )
                    ) : (
                      <Typography 
                        variant="caption" 
                        color="info.main" 
                        sx={{ mt: 1, fontSize: '0.75rem' }}
                      >
                        ReCAPTCHA verification is disabled
                      </Typography>
                    )}
                  </motion.div>

                  <motion.div variants={itemVariants}>
                    <LoginButton
                      type="submit"
                      whileHover={{ scale: isLoading ? 1 : 1.03 }}
                      whileTap={{ scale: isLoading ? 1 : 0.97 }}
                      disabled={isLoading || (ENABLE_RECAPTCHA && RECAPTCHA_SITE_KEY && !captchaVerified)}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      transition={{ duration: 0.3 }}
                      style={{
                        opacity: (isLoading || (ENABLE_RECAPTCHA && RECAPTCHA_SITE_KEY && !captchaVerified)) ? 0.7 : 1,
                      }}
                    >
                      <span>
                        {isLoading ? (
                          <>
                            <motion.div
                              animate={{ rotate: 360 }}
                              transition={{ 
                                repeat: Infinity, 
                                duration: 1,
                                ease: "linear"
                              }}
                              style={{
                                width: '20px',
                                height: '20px',
                                border: '3px solid rgba(255, 255, 255, 0.3)',
                                borderTop: '3px solid white',
                                borderRadius: '50%',
                                marginRight: '12px',
                                display: 'inline-block',
                              }}
                            />
                            Signing in...
                          </>
                        ) : (
                          'Sign In'
                        )}
                      </span>
                    </LoginButton>
                  </motion.div>
                  
                  {/* Decorative element */}
                  <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 0.5 }}
                    transition={{ delay: 1.2, duration: 0.8 }}
                    style={{
                      position: 'absolute',
                      bottom: '-70px',
                      right: '-70px',
                      width: '150px',
                      height: '150px',
                      borderRadius: '50%',
                      background: 'radial-gradient(circle, rgba(26, 115, 232, 0.1) 0%, rgba(26, 115, 232, 0) 70%)',
                      zIndex: -1
                    }}
                  />
                </FormContainer>
              </motion.div>
            )}
          </AnimatePresence>
        </RightPanel>
      </LoginContainer>
    </ThemeProvider>
  )
}

export default LoginPage
