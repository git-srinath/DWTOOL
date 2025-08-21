'use client'
import React, { useState, useEffect } from 'react'
import {
  Typography,
  Grid,
  Box,
  Container,
  Paper,
  useMediaQuery,
  Divider,
} from '@mui/material'
import { useRouter } from 'next/navigation'
import { useTheme as useMuiTheme } from '@mui/material/styles'
import { useTheme } from '@/context/ThemeContext'
import { motion, AnimatePresence } from 'framer-motion'
import {
  AdminPanelSettings,
  Storage,
  Timeline,
  AutoFixHigh,
  Shield,
  BarChart,
  TaskAlt,
  AccountCircle,
  Dashboard,
} from '@mui/icons-material'

const Page = () => {
  const router = useRouter()
  const { darkMode } = useTheme()
  const muiTheme = useMuiTheme()
  const isMobile = useMediaQuery(muiTheme.breakpoints.down('sm'))
  const isTablet = useMediaQuery(muiTheme.breakpoints.down('md'))
  const [hoveredCard, setHoveredCard] = useState(null)
  const [isLoaded, setIsLoaded] = useState(false)

  useEffect(() => {
    setIsLoaded(true)
  }, [])

  const cards = [
    {
      title: 'Data Mapper',
      path: '/mapper_module',
      gradient: 'linear-gradient(135deg, #6366F1 0%, #4F46E5 100%)',
      description:
        'Map and transform your data structures with intuitive visual tools',
      icon: <Storage sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.1,
    },
    {
      title: 'Jobs',
      path: '/jobs',
      gradient: 'linear-gradient(135deg, #0EA5E9 0%, #0284C7 100%)',
      description: 'Schedule, manage and monitor all your processing jobs',
      icon: <Timeline sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.2,
    },
    {
      title: 'Parameter Mapping',
      path: '/type_mapper',
      gradient: 'linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%)',
      description: 'Convert and validate data types with automated mapping',
      icon: <AutoFixHigh sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.3,
    },
    {
      title: 'Admin',
      path: '/admin',
      gradient: 'linear-gradient(135deg, #EC4899 0%, #DB2777 100%)',
      description: 'Comprehensive administrative tools for user management',
      icon: <AdminPanelSettings sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.4,
    },
    {
      title: 'Jobs and Status',
      path: '/job-status',
      gradient: 'linear-gradient(135deg, #10B981 0%, #059669 100%)',
      description: 'Track and manage your job executions and their status',
      icon: <TaskAlt sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.6,
    },
    {
      title: 'User Profile',
      path: '/profile',
      gradient: 'linear-gradient(135deg, #3B82F6 0%, #2563EB 100%)',
      description: 'Manage your account settings and preferences',
      icon: <AccountCircle sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.7,
    },
    {
      title: 'Dashboard',
      path: '/dashboard',
      gradient: 'linear-gradient(135deg, #8B5CF6 0%, #6D28D9 100%)',
      description: 'View summary analytics and key performance indicators',
      icon: <Dashboard sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.8,
    },
    {
      title: 'Security',
      path: '/security',
      gradient: 'linear-gradient(135deg, #64748B 0%, #475569 100%)',
      description: 'Comprehensive security controls and audit tools',
      icon: <Shield sx={{ fontSize: '2rem', color: '#fff' }} />,
      delay: 0.9,
    },
  ]

  // Animation variants
  const pageVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        duration: 0.6,
        when: 'beforeChildren',
      },
    },
  }

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.08,
        delayChildren: 0.3,
      },
    },
  }

  const cardVariants = {
    hidden: { y: 30, opacity: 0 },
    visible: (i) => ({
      y: 0,
      opacity: 1,
      transition: {
        type: 'spring',
        stiffness: 80,
        damping: 12,
        delay: i * 0.05,
      },
    }),
  }

  const headerVariants = {
    hidden: { y: -40, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
      transition: {
        type: 'spring',
        stiffness: 70,
        damping: 12,
        delay: 0.2,
      },
    },
  }

  return (
    <motion.div
      initial="hidden"
      animate={isLoaded ? 'visible' : 'hidden'}
      variants={pageVariants}
      className={`min-h-screen ${darkMode ? 'bg-[#0A101F]' : 'bg-[#F8FAFC]'}`}
    >
      <div className="absolute top-0 left-0 right-0 h-[40vh] overflow-hidden -z-10">
        <div
          className={`w-full h-full ${
            darkMode
              ? 'bg-gradient-to-b from-indigo-900/30 via-blue-900/20 to-transparent'
              : 'bg-gradient-to-b from-blue-50 via-indigo-50/50 to-transparent'
          }`}
        />
        {/* Animated background elements */}
        <div className="absolute top-0 left-0 w-full h-full overflow-hidden">
          {[...Array(8)].map((_, i) => (
            <motion.div
              key={i}
              className={`absolute rounded-full ${
                darkMode ? 'bg-blue-500/10' : 'bg-indigo-400/10'
              }`}
              style={{
                width: `${Math.random() * 160 + 40}px`,
                height: `${Math.random() * 160 + 40}px`,
                top: `${Math.random() * 100}%`,
                left: `${Math.random() * 100}%`,
              }}
              animate={{
                y: [0, Math.random() * 25 - 12],
                x: [0, Math.random() * 25 - 12],
                scale: [1, Math.random() * 0.3 + 0.9],
              }}
              transition={{
                duration: Math.random() * 5 + 10,
                repeat: Infinity,
                repeatType: 'reverse',
              }}
            />
          ))}
        </div>
      </div>

      <Container
        maxWidth="lg"
        sx={{ py: { xs: 3, md: 5 }, position: 'relative' }}
      >
        {/* Header Section with centered title */}
        <motion.div variants={headerVariants} className="text-center mb-10">
          <Typography
            variant="h2"
            component="h1"
            sx={{
              fontWeight: 800,
              mb: 1.5,
              fontSize: { xs: '2rem', md: '2.8rem' },
              background: darkMode
                ? 'linear-gradient(to right, #C4B5FD, #818CF8, #60A5FA)'
                : 'linear-gradient(to right, #4F46E5, #6366F1, #3B82F6)',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent',
              display: 'inline-block',
              letterSpacing: '-0.01em',
              lineHeight: 1.2,
            }}
          >
            Data Warehouse Tool
          </Typography>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.4 }}
          >
            <Typography
              variant="subtitle1"
              sx={{
                color: darkMode
                  ? 'rgba(255,255,255,0.7)'
                  : 'rgba(71,85,105,0.9)',
                maxWidth: '650px',
                mx: 'auto',
                mt: 2,
                mb: 3,
                lineHeight: 1.5,
                fontWeight: 400,
                fontSize: '0.95rem',
              }}
            >
              Powerful tools to transform, manage, and analyze your data
              securely and efficiently
            </Typography>
          </motion.div>
        </motion.div>

        {/* Cards Section */}
        <Box sx={{ mb: 6, mt: 2 }}>
          <motion.div variants={headerVariants} className="text-center mb-6">
            <Typography
              variant="h5"
              sx={{
                fontWeight: 700,
                mb: 1.5,
                position: 'relative',
                display: 'inline-block',
                fontSize: { xs: '1.3rem', md: '1.5rem' },
                '&::after': {
                  content: '""',
                  display: 'block',
                  width: '50px',
                  height: '3px',
                  background: darkMode
                    ? 'linear-gradient(to right, #818CF8, #6366F1)'
                    : 'linear-gradient(to right, #4F46E5, #6366F1)',
                  borderRadius: '2px',
                  margin: '8px auto 0',
                },
              }}
            >
              Access Your Tools
            </Typography>
            <Typography
              variant="body2"
              sx={{
                maxWidth: '550px',
                mx: 'auto',
                color: darkMode
                  ? 'rgba(255,255,255,0.7)'
                  : 'rgba(71,85,105,0.9)',
                fontSize: '0.85rem',
              }}
            >
              All the tools you need to transform, manage, and analyze your
              enterprise data
            </Typography>
          </motion.div>

          <motion.div variants={containerVariants} className="relative z-10">
            <Grid container spacing={2}>
              {cards.map((card, index) => (
                <Grid item xs={12} sm={6} md={3} key={index}>
                  <motion.div custom={index} variants={cardVariants}>
                    <Paper
                      component={motion.div}
                      whileHover={{
                        scale: 1.03,
                        y: -4,
                        boxShadow: darkMode
                          ? '0 16px 32px -8px rgba(0,0,0,0.5)'
                          : '0 16px 32px -8px rgba(0,0,0,0.15)',
                      }}
                      whileTap={{ scale: 0.98 }}
                      onMouseEnter={() => setHoveredCard(index)}
                      onMouseLeave={() => setHoveredCard(null)}
                      onClick={() => router.push(card.path)}
                      sx={{
                        height: { xs: 120, sm: 130, md: 130 },
                        cursor: 'pointer',
                        position: 'relative',
                        overflow: 'hidden',
                        borderRadius: '12px',
                        border: darkMode
                          ? '1px solid rgba(255,255,255,0.05)'
                          : '1px solid rgba(0,0,0,0.03)',
                        boxShadow: darkMode
                          ? '0 8px 24px rgba(0,0,0,0.25)'
                          : '0 8px 24px rgba(0,0,0,0.08)',
                        transition: 'all 0.3s ease-in-out',
                        backgroundColor: darkMode
                          ? 'rgba(15, 23, 42, 0.5)'
                          : 'rgba(255, 255, 255, 0.8)',
                        backdropFilter: 'blur(8px)',
                      }}
                    >
                      {/* Background gradient */}
                      <Box
                        sx={{
                          background: card.gradient,
                          height: '100%',
                          width: '100%',
                          position: 'absolute',
                          opacity: 0.95,
                          transition:
                            'transform 0.5s ease-out, opacity 0.3s ease-in-out',
                          transform:
                            hoveredCard === index ? 'scale(1.1)' : 'scale(1)',
                        }}
                      />

                      {/* Glass-like overlay effect */}
                      <Box
                        sx={{
                          background: darkMode
                            ? 'linear-gradient(135deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0.05) 100%)'
                            : 'linear-gradient(135deg, rgba(255,255,255,0.25) 0%, rgba(255,255,255,0.15) 100%)',
                          height: '100%',
                          width: '100%',
                          position: 'absolute',
                          transition: 'opacity 0.3s ease-in-out',
                          opacity: hoveredCard === index ? 1 : 0,
                        }}
                      />

                      {/* Content */}
                      <Box
                        sx={{
                          height: '100%',
                          display: 'flex',
                          flexDirection: 'column',
                          justifyContent: 'center',
                          alignItems: 'center',
                          position: 'relative',
                          zIndex: 2,
                          p: { xs: 1.2, md: 1.5 },
                        }}
                      >
                        <motion.div
                          animate={{
                            scale: hoveredCard === index ? 1.08 : 1,
                            y: hoveredCard === index ? -2 : 0,
                          }}
                          transition={{
                            duration: 0.4,
                            type: 'spring',
                            stiffness: 120,
                          }}
                          style={{
                            marginBottom: '6px',
                            backgroundColor: 'rgba(255, 255, 255, 0.2)',
                            borderRadius: '50%',
                            padding: '6px',
                            display: 'flex',
                          }}
                        >
                          {/* Icon */}
                          {React.cloneElement(card.icon, {
                            sx: { fontSize: '1.6rem', color: '#fff' },
                          })}
                        </motion.div>
                        <Typography
                          variant="subtitle1"
                          component="div"
                          align="center"
                          sx={{
                            color: '#fff',
                            fontWeight: 700,
                            mb: 0.5,
                            fontSize: { xs: '0.8rem', md: '0.9rem' },
                            letterSpacing: '0.2px',
                            textShadow: '0 1px 3px rgba(0,0,0,0.1)',
                          }}
                        >
                          {card.title}
                        </Typography>
                        <Typography
                          variant="caption"
                          align="center"
                          sx={{
                            color: 'rgba(255,255,255,0.95)',
                            lineHeight: 1.3,
                            fontSize: { xs: '0.65rem', md: '0.7rem' },
                            maxWidth: '95%',
                            fontWeight: 400,
                          }}
                        >
                          {card.description}
                        </Typography>

                        {/* Hover animation element */}
                        <motion.div
                          style={{
                            position: 'absolute',
                            bottom: '8px',
                            right: '8px',
                            width: '4px',
                            height: '4px',
                            borderRadius: '50%',
                            backgroundColor: 'rgba(255,255,255,0.7)',
                            opacity: 0,
                          }}
                          animate={{
                            opacity: hoveredCard === index ? 1 : 0,
                            scale: hoveredCard === index ? [1, 1.5, 1] : 1,
                          }}
                          transition={{
                            duration: 1.5,
                            repeat: hoveredCard === index ? Infinity : 0,
                            repeatType: 'loop',
                          }}
                        />
                      </Box>
                    </Paper>
                  </motion.div>
                </Grid>
              ))}
            </Grid>
          </motion.div>
        </Box>

        {/* Footer Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.6 }}
        >
          <Box sx={{ mt: 6, textAlign: 'center' }}>
            <Divider
              sx={{
                mb: 3,
                opacity: darkMode ? 0.1 : 0.2,
                maxWidth: '400px',
                mx: 'auto',
              }}
            />
            <Typography
              variant="caption"
              sx={{
                color: darkMode ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
                fontSize: '0.75rem',
                mt: 1.5,
              }}
            >
              © 2025 Ahana Systems & Solutions Pvt Ltd | All Rights Reserved
            </Typography>
          </Box>
        </motion.div>
      </Container>
    </motion.div>
  )
}

export default Page
