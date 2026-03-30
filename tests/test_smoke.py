import os

def test_project_structure():
    """Verify that the required project directories exist."""
    expected_dirs = [
        "contracts",
        "generated_contracts",
        "validation_reports",
        "violation_log",
        "schema_snapshots",
        "enforcer_report",
        "outputs",
        "tests"
    ]
    for d in expected_dirs:
        assert os.path.isdir(d), f"Directory {d} is missing"

def test_dependencies_import():
    """Verify that key dependencies can be imported."""
    import pandas
    import numpy
    import yaml
    import sklearn
    import git
    
    import importlib.metadata
    assert pandas.__version__ is not None
    assert numpy.__version__ is not None
    assert yaml.__version__ is not None
    assert importlib.metadata.version("jsonschema") is not None
    assert sklearn.__version__ is not None
    assert git.__version__ is not None

def test_generator_script():
    """Verify that the generator script runs and creates a file."""
    # Move to the directory relative to the script if needed, but here we assume we're in the project root
    from contracts.generator import generate_sample_contract
    
    contract_path = generate_sample_contract("test_smoke")
    assert os.path.exists(contract_path)
    # Cleanup
    if os.path.exists(contract_path):
        os.remove(contract_path)
