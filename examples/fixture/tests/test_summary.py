import reference_app.labels
import reference_app.summary


def test_task_b_contract() -> None:
    reference_app.summary.public_summary(reference_app.labels.public_labels())
