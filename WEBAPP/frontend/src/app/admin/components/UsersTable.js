import React, { useState } from 'react';
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Typography,
  Paper,
  IconButton,
  Tooltip,
  Chip,
  TextField,
  Button,
  Stack,
  Avatar,
  useTheme,
  alpha,
  Badge
} from '@mui/material';
import { motion } from 'framer-motion';
import SearchIcon from '@mui/icons-material/Search';
import AddIcon from '@mui/icons-material/Add';
import FilterListIcon from '@mui/icons-material/FilterList';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import CheckIcon from '@mui/icons-material/Check';
import BlockIcon from '@mui/icons-material/Block';

// Animation variants
const tableRowVariants = {
  hidden: { opacity: 0, x: -5 },
  visible: i => ({
    opacity: 1,
    x: 0,
    transition: {
      delay: i * 0.03, // stagger effect
      duration: 0.2,
      ease: 'easeInOut'
    }
  }),
  hover: { 
    backgroundColor: 'rgba(0, 0, 0, 0.04)',
    scale: 1.002,
    transition: { duration: 0.1 }
  },
  tap: { scale: 0.99 }
};

const UserTableRow = ({ user, index, onEdit, onDelete, onResetPassword, onApprove, onReject }) => {
  const theme = useTheme();
  
  const isPending = user.account_status === 'PENDING';

  return (
    <TableRow
      component={motion.tr}
      variants={tableRowVariants}
      initial="hidden"
      animate="visible"
      custom={index}
      whileHover="hover"
      sx={{ 
        height: '45px',
        '&:last-child td, &:last-child th': { border: 0 },
        borderRadius: '8px',
        '& td': { borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`, py: 1 },
        '&:nth-of-type(odd)': {
          backgroundColor: alpha(theme.palette.action.hover, 0.04),
        },
      }}
    >
      <TableCell component="th" scope="row" sx={{ py: 0.8 }}>
        <Box display="flex" alignItems="center" gap={1.2}>
          {isPending ? (
            <Badge
              overlap="circular"
              anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
              badgeContent={
                <Box
                  sx={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    backgroundColor: theme.palette.warning.main,
                    border: `2px solid ${theme.palette.background.paper}`,
                  }}
                />
              }
            >
              <Avatar
                sx={{
                  width: 28,
                  height: 28,
                  backgroundColor: alpha(theme.palette.warning.main, 0.1),
                  color: theme.palette.warning.main,
                  fontWeight: 600,
                  fontSize: '0.8rem',
                  boxShadow: `0 0 0 1px ${alpha(theme.palette.warning.main, 0.3)}`
                }}
              >
                {user.first_name ? user.first_name[0] : user.username ? user.username[0].toUpperCase() : '?'}
              </Avatar>
            </Badge>
          ) : (
            <Badge
              overlap="circular"
              anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
              badgeContent={
                <Box
                  sx={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    backgroundColor: user.is_active ? theme.palette.success.main : theme.palette.error.main,
                    border: `2px solid ${theme.palette.background.paper}`,
                  }}
                />
              }
            >
              <Avatar
                sx={{
                  width: 28,
                  height: 28,
                  background: `linear-gradient(135deg, ${theme.palette.primary.main} 0%, ${alpha(theme.palette.primary.light, 0.8)} 100%)`,
                  fontWeight: 600,
                  fontSize: '0.8rem',
                }}
              >
                {user.first_name ? user.first_name[0] : user.username ? user.username[0].toUpperCase() : '?'}
              </Avatar>
            </Badge>
          )}

          <Box>
            <Typography variant="subtitle2" fontWeight={600} noWrap sx={{ fontSize: '0.825rem' }}>
              {user.first_name} {user.last_name}
            </Typography>
            <Typography variant="caption" color="text.secondary" noWrap sx={{ fontSize: '0.7rem' }}>
              {user.username}
            </Typography>
          </Box>
        </Box>
      </TableCell>
      
      <TableCell sx={{ py: 0.8 }}>
        <Typography variant="body2" noWrap sx={{ fontSize: '0.8rem' }}>
          {user.email}
        </Typography>
      </TableCell>
      
      <TableCell sx={{ py: 0.8 }}>
        <Chip
          label={user.role_name || 'No role'}
          size="small"
          sx={{
            backgroundColor: alpha(theme.palette.primary.main, 0.1),
            color: theme.palette.primary.main,
            fontWeight: 500,
            fontSize: '0.7rem',
            height: 20,
            borderRadius: '4px',
          }}
        />
      </TableCell>
      
      <TableCell sx={{ py: 0.8 }}>
        {isPending ? (
          <Chip
            label="Pending"
            size="small"
            color="warning"
            sx={{
              fontWeight: 500,
              fontSize: '0.7rem',
              height: 20,
              borderRadius: '4px',
            }}
          />
        ) : (
          <Chip
            label={user.is_active ? 'Active' : 'Inactive'}
            size="small"
            color={user.is_active ? 'success' : 'error'}
            sx={{
              fontWeight: 500,
              fontSize: '0.7rem',
              height: 20,
              borderRadius: '4px',
            }}
          />
        )}
      </TableCell>
      
      <TableCell align="right" sx={{ py: 0.8 }}>
        <Stack direction="row" spacing={0.5} justifyContent="flex-end">
          {isPending ? (
            <>
              <Tooltip title="Approve">
                <IconButton
                  component={motion.button}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  color="success"
                  size="small"
                  onClick={() => onApprove(user.user_id)}
                  sx={{
                    p: 0.5,
                    backgroundColor: alpha(theme.palette.success.main, 0.1),
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.success.main, 0.2),
                    }
                  }}
                >
                  <CheckIcon sx={{ fontSize: 16 }} />
                </IconButton>
              </Tooltip>
              <Tooltip title="Reject">
                <IconButton
                  component={motion.button}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  color="error"
                  size="small"
                  onClick={() => onReject(user.user_id)}
                  sx={{
                    p: 0.5,
                    backgroundColor: alpha(theme.palette.error.main, 0.1),
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.error.main, 0.2),
                    }
                  }}
                >
                  <BlockIcon sx={{ fontSize: 16 }} />
                </IconButton>
              </Tooltip>
            </>
          ) : (
            <>
              <Tooltip title="Edit User">
                <IconButton
                  component={motion.button}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  color="primary"
                  size="small"
                  onClick={() => onEdit(user)}
                  sx={{
                    p: 0.5,
                    backgroundColor: alpha(theme.palette.primary.main, 0.1),
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.primary.main, 0.2),
                    }
                  }}
                >
                  <EditIcon sx={{ fontSize: 16 }} />
                </IconButton>
              </Tooltip>
              <Tooltip title="Delete User">
                <IconButton
                  component={motion.button}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  color="error"
                  size="small"
                  onClick={() => onDelete(user.user_id)}
                  sx={{
                    p: 0.5,
                    backgroundColor: alpha(theme.palette.error.main, 0.1),
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.error.main, 0.2),
                    }
                  }}
                >
                  <DeleteIcon sx={{ fontSize: 16 }} />
                </IconButton>
              </Tooltip>
              <Tooltip title="Reset Password">
                <IconButton
                  component={motion.button}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  color="info"
                  size="small"
                  onClick={() => onResetPassword(user)}
                  sx={{
                    p: 0.5,
                    backgroundColor: alpha(theme.palette.info.main, 0.1),
                    '&:hover': {
                      backgroundColor: alpha(theme.palette.info.main, 0.2),
                    }
                  }}
                >
                  <RestartAltIcon sx={{ fontSize: 16 }} />
                </IconButton>
              </Tooltip>
            </>
          )}
        </Stack>
      </TableCell>
    </TableRow>
  );
};

const UsersTable = ({ 
  users, 
  pendingApprovals = [], 
  onOpenNewUserDialog, 
  onEdit, 
  onDelete, 
  onResetPassword,
  onApprove,
  onReject
}) => {
  const theme = useTheme();
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [searchTerm, setSearchTerm] = useState('');
  
  // Combine users and pending approvals
  const allUsers = [...users, ...pendingApprovals];
  
  // Filter users based on search term
  const filteredUsers = searchTerm 
    ? allUsers.filter(user => 
        user.username?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        user.email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        user.first_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        user.last_name?.toLowerCase().includes(searchTerm.toLowerCase())
      )
    : allUsers;
  
  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };
  
  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };
  
  return (
    <Box sx={{ width: '100%' }}>
      <Box sx={{ mb: 2 }}>
        <Paper
          elevation={0}
          sx={{
            p: 1.5,
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 1.5,
            borderRadius: 3,
            backdropFilter: 'blur(10px)',
            background: alpha(theme.palette.background.paper, 0.8),
            boxShadow: `0 6px 16px 0 ${alpha(theme.palette.primary.main, 0.1)}`,
          }}
        >
          <TextField
            placeholder="Search users..."
            variant="outlined"
            size="small"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary', fontSize: '1.1rem' }} />,
              sx: {
                borderRadius: 2,
                height: 36,
                backgroundColor: theme.palette.background.paper,
                '& .MuiOutlinedInput-notchedOutline': {
                  borderColor: alpha(theme.palette.divider, 0.4),
                }
              }
            }}
            sx={{ flex: 1, minWidth: { xs: '100%', sm: 200 } }}
          />
          
          <Stack direction="row" spacing={1} sx={{ ml: { xs: 0, sm: 'auto' } }}>
            <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
              <Button
                variant="outlined"
                startIcon={<FilterListIcon />}
                sx={{
                  borderRadius: 2,
                  px: 1.5,
                  py: 0.6,
                  backgroundColor: alpha(theme.palette.background.paper, 0.6),
                  borderColor: alpha(theme.palette.divider, 0.4),
                  color: theme.palette.text.primary,
                  fontSize: '0.8rem',
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.background.paper, 0.8),
                    borderColor: alpha(theme.palette.primary.main, 0.6),
                  },
                }}
              >
                Filters
              </Button>
            </motion.div>
            
            <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={onOpenNewUserDialog}
                sx={{
                  borderRadius: 2,
                  px: 1.5,
                  py: 0.6,
                  fontSize: '0.8rem',
                  background: `linear-gradient(90deg, ${theme.palette.primary.main}, ${alpha(theme.palette.primary.dark, 0.9)})`,
                  boxShadow: `0 3px 8px ${alpha(theme.palette.primary.main, 0.4)}`,
                  color: theme.palette.primary.contrastText,
                  '&:hover': {
                    boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.6)}`,
                  },
                }}
              >
                Add User
              </Button>
            </motion.div>
          </Stack>
        </Paper>
      </Box>
      
      <TableContainer
        component={motion.div}
        initial={{ opacity: 0, y: 15 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        sx={{
          borderRadius: 3,
          boxShadow: `0 6px 16px 0 ${alpha(theme.palette.common.black, 0.08)}`,
          backgroundColor: alpha(theme.palette.background.paper, 0.95),
          backdropFilter: 'blur(10px)',
          overflow: 'hidden',
        }}
      >
        <Table size="small" sx={{ minWidth: 650 }}>
          <TableHead>
            <TableRow
              sx={{
                backgroundColor: alpha(theme.palette.primary.main, 0.03),
                '& th': {
                  borderBottom: `2px solid ${alpha(theme.palette.primary.main, 0.1)}`,
                  color: theme.palette.text.secondary,
                  fontWeight: 600,
                  fontSize: '0.7rem',
                  textTransform: 'uppercase',
                  letterSpacing: '0.5px',
                  py: 1,
                },
              }}
            >
              <TableCell>User</TableCell>
              <TableCell>Email</TableCell>
              <TableCell>Role</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredUsers
              .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
              .map((user, index) => (
                <UserTableRow
                  key={user.user_id}
                  user={user}
                  index={index}
                  onEdit={onEdit}
                  onDelete={onDelete}
                  onResetPassword={onResetPassword}
                  onApprove={onApprove}
                  onReject={onReject}
                />
              ))}
            
            {filteredUsers.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 2 }}>
                  <Box>
                    <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                      No users found
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.8rem' }}>
                      {searchTerm ? 'Try adjusting your search criteria' : 'Add users to get started'}
                    </Typography>
                  </Box>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'flex-end',
            alignItems: 'center',
            p: 1,
            borderTop: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
          }}
        >
          <TablePagination
            component="div"
            count={filteredUsers.length}
            page={page}
            onPageChange={handleChangePage}
            rowsPerPage={rowsPerPage}
            onRowsPerPageChange={handleChangeRowsPerPage}
            rowsPerPageOptions={[5, 10, 25, 50]}
            labelRowsPerPage=""
            sx={{
              '& .MuiTablePagination-toolbar': {
                minHeight: 40,
              },
              '& .MuiTablePagination-selectIcon': {
                width: 16,
                height: 16,
              },
              '& .MuiTablePagination-select': {
                paddingY: 0.5,
              }
            }}
          />
        </Box>
      </TableContainer>
    </Box>
  );
};

export default UsersTable; 