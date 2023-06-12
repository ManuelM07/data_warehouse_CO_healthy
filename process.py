from processing.medical_formula_process import run as medical_formula_run
from processing.payment_process import run as payment_run
from processing.retreat_process import run as retreat__run
from processing.service_process import run as service_run


# Run all process
medical_formula_run()
payment_run()
retreat__run()
service_run()