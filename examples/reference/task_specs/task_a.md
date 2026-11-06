# Task A

Make reference_app.labels.public_labels() available from the single product file named below.
tests/test_labels.py calls that behavior with no arguments.
The call is accepted when it completes and the validation command exits successfully.
No other product path is authorized.

allowed_path: src/reference_app/labels.py
behavior: reference_app.labels.public_labels()
contract_test: tests/test_labels.py
validation_command: python -m pytest -q -p no:cacheprovider tests/test_labels.py
