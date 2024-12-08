from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
import base64
import os
import yaml

# Function to derive a key from a password and salt
def derive_key_from_password(password: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,  # Key length for Fernet (32 bytes)
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

# Function to encrypt a YAML file
def encrypt_yaml(file_path, password):
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

# Function to decrypt an encrypted YAML file
def decrypt_yaml(encrypted_file_path, salt_file_path, password):
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

if __name__ == "__main__":
    # File paths
    yaml_file = "secret.yaml"
    password = input("Enter a password to encryp/decrypt the file: ")

    # Example: Encrypt the YAML file
    encrypt_yaml(yaml_file, password)

    # Example: Decrypt the YAML file
    encrypted_file = f"{yaml_file}.enc"
    salt_file = f"{yaml_file}.salt"
    
    decrypted_data = decrypt_yaml(encrypted_file, salt_file, password)
    print("Decrypted YAML data:")
    print(decrypted_data)

