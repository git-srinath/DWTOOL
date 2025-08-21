from flask import Blueprint, jsonify, request
from modules.license.license_manager import LicenseManager
from modules.login.login import token_required, hash_password
from modules.admin.admin import admin_required      
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

# Database setup
engine = create_engine(os.getenv('SQLITE_DATABASE_URL'))
Session = sessionmaker(bind=engine)

# Initialize license manager
license_manager = LicenseManager()

# Create blueprint
license_bp = Blueprint('license', __name__)

@license_bp.route('/license/status', methods=['GET'])
def get_license_status():
    """Get license status without requiring authentication"""
    try:
        status = license_manager.get_license_status()
        response = jsonify({
            'success': True,
            'data': status
        })
        
        return response
    except Exception as e:
        print(f"Error getting license status: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Failed to get license status',
            'message': str(e)
        }), 500

@license_bp.route('/admin/license/activate', methods=['POST'])
@token_required
@admin_required
def activate_license(current_user_id):
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'message': 'No JSON data provided'
            }), 400

        license_key = data.get('license_key')
        if not license_key:
            return jsonify({
                'success': False,
                'error': 'Missing license key',
                'message': 'License key is required'
            }), 400
            
        success, message = license_manager.activate_license(license_key)
        return jsonify({
            'success': success,
            'message': message
        })
    except Exception as e:
        print(f"Error activating license: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'License activation failed',
            'message': str(e)
        }), 500

@license_bp.route('/admin/license/deactivate', methods=['POST'])
@token_required
@admin_required
def deactivate_license(current_user_id):
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'message': 'No JSON data provided'
            }), 400
            
        password = data.get('password')
        if not password:
            return jsonify({
                'success': False,
                'error': 'Missing password',
                'message': 'Password is required for deactivation'
            }), 400
            
        # Verify password
        if not verify_admin_password(current_user_id, password):
            return jsonify({
                'success': False,
                'error': 'Authentication failed',
                'message': 'Incorrect password'
            }), 401
            
        success, message = license_manager.deactivate_license()
        return jsonify({
            'success': success,
            'message': message
        })
    except Exception as e:
        print(f"Error deactivating license: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'License deactivation failed',
            'message': str(e)
        }), 500

@license_bp.route('/admin/license/change', methods=['POST'])
@token_required
@admin_required
def change_license(current_user_id):
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'Invalid request',
                'message': 'No JSON data provided'
            }), 400

        license_key = data.get('license_key')
        password = data.get('password')
        
        if not license_key:
            return jsonify({
                'success': False,
                'error': 'Missing license key',
                'message': 'License key is required'
            }), 400
            
        if not password:
            return jsonify({
                'success': False,
                'error': 'Missing password',
                'message': 'Password is required for license change'
            }), 400
            
        # Verify password
        if not verify_admin_password(current_user_id, password):
            return jsonify({
                'success': False,
                'error': 'Authentication failed',
                'message': 'Incorrect password'
            }), 401
            
        # First deactivate current license
        license_manager.deactivate_license()
        
        # Then activate new license
        success, message = license_manager.activate_license(license_key)
        return jsonify({
            'success': success,
            'message': message
        })
    except Exception as e:
        print(f"Error changing license: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'License change failed',
            'message': str(e)
        }), 500

def verify_admin_password(user_id, password):
    """Verify user password"""
    try:
        session = Session()
        user = session.execute(
            text("SELECT password_hash, salt FROM users WHERE user_id = :user_id"),
            {'user_id': user_id}
        ).fetchone()
        
        if not user:
            return False
            
        # Verify password
        hashed_password = hash_password(password, user.salt)
        return hashed_password == user.password_hash
    except Exception as e:
        print(f"Error verifying password: {str(e)}")
        return False
    finally:
        session.close()

# Add CORS preflight handler
# @license_bp.route('/license/status', methods=['OPTIONS'])
# def handle_license_status_preflight():
    try:
        response = jsonify({
            'success': True,
            'message': 'Preflight request successful'
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', '*')
        response.headers.add('Access-Control-Allow-Methods', 'GET, OPTIONS')
        return response
    except Exception as e:
        print(f"Error handling preflight request: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Preflight request failed',
            'message': str(e)
        }), 500 