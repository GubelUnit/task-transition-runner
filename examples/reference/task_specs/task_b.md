# Task B

Make reference_app.summary.public_summary(reference_app.labels.public_labels()) available from the single product file named below.
tests/test_labels.py calls the labels behavior with no arguments.
tests/test_summary.py calls the summary behavior with that labels result as its only argument.
The summary call must use the labels result.
The calls are accepted when they complete and the validation commands exit successfully, labels contract first, summary contract second.
No other product path is authorized.
The summary product itself binds the labels component as a module dependency, while public_summary continues to use its single labels result parameter.

allowed_path: src/reference_app/summary.py
behavior: reference_app.summary.public_summary(reference_app.labels.public_labels())
contract_test: tests/test_labels.py
contract_test: tests/test_summary.py
validation_command: python -m pytest -q -p no:cacheprovider tests/test_labels.py
validation_command: python -m pytest -q -p no:cacheprovider tests/test_summary.py
dependency_module: reference_app.labels
