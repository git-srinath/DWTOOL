'use client';

import { useState } from 'react';
import { Container, Paper, Typography, Alert } from '@mui/material';
import { motion } from 'framer-motion';
import { styled } from '@mui/material/styles';
import Link from 'next/link';
import Input from '../../components/shared/Input';
import Button from '../../components/shared/Button';
import { useAuth } from '../../context/AuthContext';

const StyledContainer = styled(Container)`
  height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
`;

const StyledPaper = styled(Paper)`
  padding: 40px;
  width: 100%;
  max-width: 400px;
  text-align: center;
  border-radius: 15px;
  box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
`;

const Logo = styled(motion.div)`
  margin-bottom: 30px;
  font-size: 2rem;
  font-weight: bold;
  color: #2196F3;
`;

const ForgotPasswordPage = () => {
  const [email, setEmail] = useState('');
  const [status, setStatus] = useState({ type: '', message: '' });
  const { forgotPassword } = useAuth();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setStatus({ type: '', message: '' });

    const result = await forgotPassword(email);
    if (result.success) {
      setStatus({
        type: 'success',
        message: 'If an account exists with this email, you will receive password reset instructions.'
      });
      setEmail('');
    } else {
      setStatus({
        type: 'error',
        message: result.error
      });
    }
  };

  return (
    <StyledContainer>
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
      >
        <StyledPaper elevation={3}>
          <Logo
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{ type: 'spring', stiffness: 200, damping: 10 }}
          >
            Reset Password
          </Logo>

          <Typography variant="body1" sx={{ mb: 3 }}>
            Enter your email address and we'll send you instructions to reset your password.
          </Typography>

          <form onSubmit={handleSubmit}>
            {status.message && (
              <Alert severity={status.type} sx={{ mb: 2 }}>
                {status.message}
              </Alert>
            )}
            
            <Input
              label="Email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />

            <Button
              type="submit"
              fullWidth
              size="large"
              sx={{ mt: 2 }}
            >
              Send Reset Instructions
            </Button>

            <Typography variant="body2" sx={{ mt: 2 }}>
              <Link href="/auth/login" style={{ color: '#2196F3', textDecoration: 'none' }}>
                Back to Login
              </Link>
            </Typography>
          </form>
        </StyledPaper>
      </motion.div>
    </StyledContainer>
  );
};

export default ForgotPasswordPage; 