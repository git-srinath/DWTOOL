import uuid
import hashlib
import hmac
import base64
from datetime import datetime, timedelta
import json
from cryptography.fernet import Fernet
import getmac
import os
import argparse

def generate_secret_key():
    """Generate a secret key for Fernet encryption"""
    return Fernet.generate_key()

def get_system_identifier():
    """Get unique system identifier based on MAC address"""
    mac = getmac.get_mac_address()
    print(f"Retrieved MAC address: {mac}")
    return mac.replace(':', '')

class LicenseKeyGenerator:
    def __init__(self, secret_key=None):
        if secret_key is None:
            self.secret_key = generate_secret_key()
        else:
            self.secret_key = secret_key
        self.fernet = Fernet(self.secret_key)
        
    def generate_license_key(self, system_id=None, days_valid=365, features=None):
        """Generate a new license key"""
        if features is None:
            features = ["basic"]
            
        if system_id is None:
            system_id = get_system_identifier()
            
        print(f"Generating license key for system ID: {system_id}")
            
        # Create license data
        license_data = {
            "created_at": datetime.now().isoformat(),
            "valid_until": (datetime.now() + timedelta(days=days_valid)).isoformat(),
            "features": features,
            "license_id": str(uuid.uuid4()),
            "system_id": system_id
        }
        
        print(f"License data being encrypted: {json.dumps(license_data, indent=2)}")
        
        # Convert to JSON and encrypt
        json_data = json.dumps(license_data)
        encrypted_data = self.fernet.encrypt(json_data.encode())
        
        # Encode to base64 for easier handling
        license_key = base64.urlsafe_b64encode(encrypted_data).decode()
        return license_key, license_data
    
    def validate_license_key(self, license_key, system_id):
        """Validate a license key"""
        try:
            print(f"\nValidating license key...")
            print(f"Provided system ID: {system_id}")
            
            # Decode from base64
            encrypted_data = base64.urlsafe_b64decode(license_key)
            
            # Decrypt the data
            decrypted_data = self.fernet.decrypt(encrypted_data)
            license_data = json.loads(decrypted_data)
            
            print(f"Decrypted license data: {json.dumps(license_data, indent=2)}")
            
            # Check system ID
            if license_data.get('system_id') != system_id:
                print(f"System ID mismatch:")
                print(f"  License system ID: {license_data.get('system_id')}")
                print(f"  Current system ID: {system_id}")
                return False, "License key is not valid for this system"
            
            # Check expiration
            valid_until = datetime.fromisoformat(license_data['valid_until'])
            if valid_until < datetime.now():
                print(f"License expired:")
                print(f"  Valid until: {valid_until}")
                print(f"  Current time: {datetime.now()}")
                return False, "License has expired"
            
            print("License validation successful!")
            return True, license_data
            
        except Exception as e:
            print(f"Error during license validation: {str(e)}")
            return False, f"Invalid license key: {str(e)}"

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate license key with specified validity period')
    parser.add_argument('--days', type=int, default=365, help='Number of days the license will be valid')
    args = parser.parse_args()
    
    # Get the system identifier (MAC address)
    system_id = get_system_identifier()
    
    # Generate a new license key
    generator = LicenseKeyGenerator()
    license_key, license_data = generator.generate_license_key(
        system_id=system_id,
        days_valid=args.days,
        features=["basic", "advanced", "premium"]
    )
    
    # Save the secret key to a file in modules/license directory
    os.makedirs('modules/license', exist_ok=True)
    with open('modules/license/secret.key', 'wb') as f:
        f.write(generator.secret_key)
    
    # Save the license key to a file in modules/license directory
    with open('modules/license/license.key', 'w') as f:
        f.write(license_key)
    
    # Print only the requested information
    print(f"System MAC: {system_id}")
    print(f"License Key: {license_key}")
    print(f"Validation Key: {generator.secret_key.decode()}")
    print(f"Number of Days: {args.days}")
    print(f"License ID: {license_data['license_id']}")
    print(f"Created Date: {datetime.fromisoformat(license_data['created_at']).strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 