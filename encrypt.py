from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
import base64
import os
import yaml

def derive_key_from_password(password: str, salt: bytes) -> bytes:
    try:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # Key length for Fernet (32 bytes)
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))
    except Exception as e:
        print(f"Error deriving key: {e}")
        raise

def encrypt_yaml(file_path, password):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Generate a random salt
        salt = os.urandom(16)

        # Derive a key from the password
        key = derive_key_from_password(password, salt)
        fernet = Fernet(key)

        # Read the YAML file and encrypt its contents
        with open(file_path, "rb") as file:
            plaintext = file.read()
        encrypted = fernet.encrypt(plaintext)

        # Save the encrypted content and salt
        with open(f"{file_path}.enc", "wb") as enc_file:
            enc_file.write(encrypted)
        with open(f"{file_path}.salt", "wb") as salt_file:
            salt_file.write(salt)

        # Delete the original YAML file
        os.remove(file_path)
        print(f"Encrypted file saved as {file_path}.enc")
        print(f"Salt saved as {file_path}.salt")
        print(f"Original file {file_path} has been deleted.")
    except FileNotFoundError as e:
        print(f"File error: {e}")
    except PermissionError as e:
        print(f"Permission error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during encryption: {e}")

def decrypt_yaml(encrypted_file_path, salt_file_path, password):
    try:
        if not os.path.exists(encrypted_file_path):
            raise FileNotFoundError(f"Encrypted file not found: {encrypted_file_path}")
        if not os.path.exists(salt_file_path):
            raise FileNotFoundError(f"Salt file not found: {salt_file_path}")

        # Load the salt
        with open(salt_file_path, "rb") as salt_file:
            salt = salt_file.read()

        # Derive the key from the password and salt
        key = derive_key_from_password(password, salt)
        fernet = Fernet(key)

        # Load the encrypted content
        with open(encrypted_file_path, "rb") as enc_file:
            encrypted_content = enc_file.read()

        # Decrypt the content
        decrypted_content = fernet.decrypt(encrypted_content)

        # Parse the decrypted YAML
        return yaml.safe_load(decrypted_content)
    except FileNotFoundError as e:
        print(f"File error: {e}")
    except PermissionError as e:
        print(f"Permission error: {e}")
    except yaml.YAMLError as e:
        print(f"YAML parsing error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during decryption: {e}")
        raise

if __name__ == "__main__":
    try:
        # File paths
        yaml_file = "secret.yaml"

        # Request password from user
        password = input("Enter a password to encrypt/decrypt the file: ")

        # Encrypt the YAML file
        encrypt_yaml(yaml_file, password)

        # Example: Decrypt the YAML file
        encrypted_file = f"{yaml_file}.enc"
        salt_file = f"{yaml_file}.salt"
        
        decrypted_data = decrypt_yaml(encrypted_file, salt_file, password)
        print("Decrypted YAML data:")
        print(decrypted_data)
    except Exception as e:
        print(f"An error occurred in the main process: {e}")

