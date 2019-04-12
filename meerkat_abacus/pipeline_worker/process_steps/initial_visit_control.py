from dateutil.parser import parse
from sqlalchemy import and_
from sqlalchemy.exc import OperationalError
from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model, util
from meerkat_abacus import logger


class InitialVisitControl(ProcessingStep):

    def __init__(self, param_config, session):
        super().__init__()
        self.step_name = "initial_visit_control"
        self.session = session
        self.param_config = param_config

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, new_engine):
        self._engine = new_engine

    def run(self, form, data):
        """
        Configures and corrects the initial visits
        """
        param_config = self.param_config

        empty_return = [{"form": form,
                         "data": data}]
        
        if "initial_visit_control" not in param_config.country_config:
            return empty_return

        new_visit_value = "new"
        return_visit_value = "return"
        if form in param_config.country_config['initial_visit_control'].keys():

            table = model.form_tables(param_config=param_config)[form]

            identifier_key_list = param_config.country_config[
                'initial_visit_control'][form]['identifier_key_list']

            current_identifier_values = {}
            for key in identifier_key_list:
                if data[key] is None:
                    return empty_return
                current_identifier_values[key] = data[key]
            visit_type_key = param_config.country_config[
                'initial_visit_control'][form]['visit_type_key']
            if data[visit_type_key] != new_visit_value:
                return empty_return

            visit_date_key = param_config.country_config[
                'initial_visit_control'][form]['visit_date_key']
            module_key = param_config.country_config[
                'initial_visit_control'][form]['module_key']
            module_value = param_config.country_config[
                'initial_visit_control'][form]['module_value']

            if data[module_key] != module_value:
                return [{"form": form,
                         "data": data}]

            ret_corrected = self.get_initial_visits(self.session, table,
                                                    current_identifier_values,
                                                    identifier_key_list,
                                                    visit_type_key,
                                                    visit_date_key,
                                                    module_key, module_value)
            if len(ret_corrected) > 0:
                combined_data = [data] + [r.data for r in ret_corrected]
                combined_data.sort(key=lambda d: parse(d[visit_date_key]),
                                   reverse=False)
                for row in combined_data[1:]:
                    row[visit_type_key] = return_visit_value
                    #logger.info("Updated data with uuid {}".format(
                    #    row["meta/instanceID"]))  # TODO: refactor this properly
                return [{"form": form,
                         "data": row} for row in combined_data]
            else:
                return empty_return

        else:
            return empty_return
        #file_name = config.data_directory + 'initial_visit_control_corrected_rows.csv'
        #util.write_csv(log, file_name, mode="a")

        return empty_return

    def get_initial_visits(self, session, table, current_values,
                           identifier_key_list=['patientid', 'icd_code'],
                           visit_type_key='intro./visit',
                           visit_date_key='pt./visit_date',
                           module_key='intro./module', module_value="ncd"):
        """
        Finds cases where a patient has multiple initial visits.

        Args:
            session: db session
            table: table to check for duplicates
            current_values: current_values for identifier_keys
            identifier_key_list: list of json keys in the data column that should occur only once for an initial visit
            visit_type_key: key of the json column data that defines visit type
            visit_date_key: key of the json column data that stores the visit date
            module_key: module to filter the processing to
            module_value
        """
        new_visit_value = "new"
        # construct a comparison list that makes sure the identifier jsonb data values are not empty
        empty_values_filter = []
        conditions = []
        for key in identifier_key_list:
            # make a column object list of identifier values
            conditions.append(table.data[key].astext == current_values[key])

            # construct a comparison list that makes sure the identifier
            # jsonb data values are not empty
            empty_values_filter.append(table.data[key].astext != "")
        result_query = session.query(
            table.id, table.uuid,
            table.data) \
            .filter(table.data[visit_type_key].astext == new_visit_value) \
            .filter(and_(*empty_values_filter)) \
            .filter(table.data[module_key].astext == module_value)\
            .filter(*conditions)

        try:
            results = result_query.all()
        except:
            logger.info("Rolled back session")
            session.rollback()
            results = result_query.all()
        return results
