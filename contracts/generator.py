import os
import yaml

def generate_sample_contract(name: str):
    """Generates a sample YAML contract."""
    contract = {
        "contract_name": name,
        "version": "1.0.0",
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "data": {"type": "string"}
            },
            "required": ["id", "data"]
        }
    }
    
    os.makedirs("generated_contracts", exist_ok=True)
    file_path = f"generated_contracts/{name}_contract.yaml"
    with open(file_path, "w") as f:
        yaml.dump(contract, f)
    
    print(f"Contract generated at {file_path}")
    return file_path

if __name__ == "__main__":
    generate_sample_contract("sample")
