import luigi
import ast
import yaml
import json
import time
import datafs
import xarray as xr
import pandas as pd
from numba import jit

def requires(**required_kwargs):
    def decorator(func):
        def inner(self, datasets=None, **kwargs):
            for var, varname in required_kwargs.items():
                kwargs[var] = datasets[varname]

            return jit(func)(**kwargs)

        return inner
    return decorator

class ImpactLabComputer(luigi.Task):
    paramfile = luigi.Parameter()
    output_var = luigi.Parameter()
    datasets = luigi.Parameter()

    def output(self):
        return

    def run(self):

        api = datafs.get_api()

        dataset_spec = ast.literal_eval(self.datasets)

        datasets = {}

        for var_name, arch_name in dataset_spec.items():
            archive = api.get_archive(arch_name)

            with archive.get_local_path() as f:
                with xr.open_dataset(f) as ds:
                    datasets[var_name] = ds.load()
                    ds.close()

        param_set = pd.read_csv(self.paramfile, header=None)

        for i in range(len(param_set)):
            parameters = param_set.iloc[i].values
            
            outputter = self.action(parameters=parameters, **datasets)

            dest = api.create(
                '{}_{}.csv'.format(
                    self.output_var, 
                    '_'.join(list(map(str, parameters)))),
                metadata=dict(
                    description='Example outputs'),
                tags=self.output_var.split('_'),
                raise_if_exists=False)

            with dest.open('wb+') as f:
                outputter(f)


class ScenarioRunner(luigi.Task):

    def requires(self):
        deps = []

        class ScenarioComputer(ImpactLabComputer):
            
            @staticmethod
            def action(*args, **kwargs):
                return self.action(*args, **kwargs)

        with open(self.varfile, 'r') as f:
            jobspec = yaml.load(f.read())

        for job in jobspec['jobs']:
            deps.append(ScenarioComputer(
                self.paramfile,
                datasets=json.dumps(job['inputs']),
                output_var=job['output']))

        return deps

    def run(self):
        for task in self.input():
            print(task.output_var)