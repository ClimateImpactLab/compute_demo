
import impactlab

class Mortality(impactlab.ScenarioRunner):
    varfile = 'mortality.yml'
    paramfile = 'my_parameters.txt'

    def action(self, tas, parameters):

        gamma1, gamma2, gamma3 = parameters

        result = (gamma1*(tas.tas**2) + gamma2*tas.tas + gamma3).to_pandas()

        def outputter(filepath):
            result.to_csv(filepath)

        return outputter
