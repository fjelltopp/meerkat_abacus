import logging


from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import util
from meerkat_abacus.util import data_types


class ToDataType(ProcessingStep):

    def __init__(self, param_config):
        self.config = param_config
        self.links_by_type, self.links_by_name = util.get_links(
            param_config.config_directory +
            param_config.country_config["links_file"])

    def run(self, form, data):
        """
        Creates all the links for a given data row
        Args:
        data_type: The data type we are working with
        input_conditions: Some data types have conditions for
                          which records qualify
        table: Class of the table we are linking from
        session: Db session
        conn: DB connection

        """
        return_data = []
        # from nose.tools import set_trace; set_trace()
        for data_type in data_types.data_types(param_config=self.config):
            main_form = data_type["form"]
            additional_forms = []
            for link in self.links_by_type.get(data_type["name"], []):
                additional_forms.append(link["to_form"])
            d = {"type": data_type["type"]}
            if form == main_form:
                if check_data_type_condition(data_type, data):
                    d["raw_data"] = data
                else:
                    continue
            elif form in additional_forms:
                d["link_data"] = (form, data)
            else:
                continue
            return_data.append({"form": "data",
                                "data": d})

        return return_data

    
def check_data_type_condition(data_type, data):
    if data_type["db_column"] and data:
        if data.get(data_type["db_column"]) == data_type["condition"]:
            return True
    else:
        return True
    return False
