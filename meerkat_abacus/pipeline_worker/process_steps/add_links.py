import logging
from sqlalchemy.orm import Query
from sqlalchemy import func
from dateutil.parser import parse

from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model, util


class AddLinks(ProcessingStep):

    def __init__(self, param_config, engine, session):
        self.step_name = "add_links"
        self.config = param_config
        self.session = session
        self.engine = engine
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

        if "raw_data" in data:
            new_data = [data]
        elif "link_data" in data:
            new_data = self._get_from_links(data)
        return_data = []
        for d in new_data:
            return_data.append({"form": "data",
                                "data": self._get_to_links(d)})
        return return_data

    def _get_from_links(self, data):
        return_data = []
        assert len(data["link_data"].keys()) == 1
        link = self.links_by_name[list(data["link_data"].keys())[0]]
        link_data = data["link_data"][link["name"]][0]
        if link.get("to_condition"):
            column, condition = link["to_condition"].split(":")
            if link_data.get(column) != condition:
                return []
        # aggregate_condition = link['aggregate_condition'] TODO
        from_form = model.form_tables(
            param_config=self.config)[link["from_form"]]
        # link_names.append(link["name"])

        columns = [from_form.uuid, from_form.data]
        conditions = []
        for i in range(len(link["from_column"].split(";"))):
            from_column = link["from_column"].split(";")[i]
            to_column = link["to_column"].split(";")[i]
            operator = link["method"].split(";")[i]
            if operator == "match":
                conditions.append(from_form.data[from_column].astext ==
                                  link_data[to_column])

            elif operator == "lower_match":
                conditions.append(
                    func.replace(
                        func.lower(
                            from_form.data[from_column].astext),
                        "-", "_") ==
                    str(link_data.get(to_column)).lower().replace("-",
                                                                  "_"))

            elif operator == "alert_match":
                id_length = self.config.country_config["alert_id_length"]
                conditions.append(
                    func.substring(
                        from_form.data[from_column].astext,
                        42 - id_length, id_length
                        ) == link_data[to_column])

            conditions.append(from_form.data[from_column].astext != '')
        try:
            link_query = self.session.query(*columns).filter(*conditions).all()
        except:
            self.session.rollback()
            link_query = self.session.query(*columns).filter(*conditions).all()
        for base_form_value in link_query:
            return_data.append(
                {"type": data["type"],
                 "original_form": link["from_form"],
                 "raw_data":  base_form_value[1],
                 "link_data": {link["name"]: link_data}
                })
        return return_data

    def _get_to_links(self, data):
        link_data = {}
        for link in self.links_by_type.get(data["type"], []):
            to_form = model.form_tables(
                param_config=self.config)[link["to_form"]]

            columns = [to_form.uuid, to_form.data]
            conditions = []
            for i in range(len(link["from_column"].split(";"))):
                from_column = link["from_column"].split(";")[i]
                to_column = link["to_column"].split(";")[i]
                operator = link["method"].split(";")[i]
                if operator == "match":
                    try:
                        conditions.append(to_form.data[to_column].astext ==
                                          data["raw_data"][from_column])
                    except:
                        logging.info("ERROR")
                        logging.info(data["raw_data"])
                elif operator == "lower_match":
                    conditions.append(
                        func.replace(
                            func.lower(to_form.data[to_column].astext),
                            "-", "_") ==
                        str(data["raw_data"][from_column]).lower().replace("-", "_"))

                elif operator == "alert_match":
                    conditions.append(
                        to_form.data[to_column].astext ==
                        data["raw_data"][from_column][-self.config.country_config["alert_id_length"]:])
#                conditions.append(to_form.uuid != uuid)
                conditions.append(to_form.data[to_column].astext != '')

            # handle the filter condition
            if link.get("to_condition", None):
                column, condition = link["to_condition"].split(":")
                conditions.append(
                    to_form.data[column].astext == condition)
            try:
                link_query = self.session.query(*columns).filter(*conditions).all()
            except:
                self.session.rollback()
                link_query = self.session.query(*columns).filter(*conditions).all()
            if link["name"] in data.get("link_data", {}):
                link_query.append( (None, data["link_data"][link["name"]]))
            if len(link_query) > 1:
                # Want to correctly order the linked forms
                column, method = link["order_by"].split(";")
                if method == "date":
                    sort_function = lambda x: parse(x[1][column])
                else:
                    sort_function = lambda x: x[1][column]
                link_query = sorted(link_query, key=sort_function)
            if len(link_query) > 0:
                link_data[link["name"]] = [l[1] for l in link_query]

        data["link_data"] = link_data
        return data
