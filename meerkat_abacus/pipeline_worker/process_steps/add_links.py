import logging
from sqlalchemy import func
from dateutil.parser import parse

from meerkat_abacus.pipeline_worker.process_steps import ProcessingStep
from meerkat_abacus import model, util


class AddLinks(ProcessingStep):

    def __init__(self, param_config, engine, session):
        super().__init__()
        self.step_name = "add_links"
        self.config = param_config
        self.session = session
        self.engine = engine
        links_file_ = param_config.config_directory \
                      + param_config.country_config["links_file"]
        self.links_by_type, self.links_by_name = util.get_links(links_file_)

    def run(self, form, data):
        """
        Creates all the links for a given data row
        """

        new_data = []
        if "raw_data" in data:
            new_data = [data]
        elif "link_data" in data:
            new_data = self._get_from_links(data)

        return_data = [{
            "form": "data",
            "data": self._get_to_links(data)
        } for data in new_data]
        return return_data

    def _get_from_links(self, data):
        assert len(data["link_data"].keys()) == 1

        link_name_ = list(data["link_data"].keys())[0]
        link = self.links_by_name[link_name_]
        link_data = data["link_data"][link["name"]][0]
        if link.get("to_condition"):
            column, condition = link["to_condition"].split(":")
            if link_data.get(column) != condition:
                return []
        # aggregate_condition = link['aggregate_condition'] TODO
        from_form_name_ = link["from_form"]
        from_form = model.form_tables(param_config=self.config)[from_form_name_]
        # link_names.append(link["name"])

        columns = [from_form.uuid, from_form.data]
        conditions = []
        from_columns = link["from_column"].split(";")
        to_columns = link["to_column"].split(";")
        methods = link["method"].split(";")
        for from_column, to_column, method in zip(from_columns, to_columns, methods):
            from_column_text = from_form.data[from_column].astext
            expected_value = link_data.get(to_column)

            conditions.append(from_column_text != '')
            if method == "match":
                condition_ = from_column_text == expected_value
                conditions.append(condition_)
            elif method == "lower_match":
                lower_ = func.lower(from_column_text)
                left = func.replace(lower_, "-", "_")
                right = str(expected_value).lower().replace("-", "_")
                conditions.append(left == right)
            elif method == "alert_match":
                id_length = self.config.country_config["alert_id_length"]
                index_ = 42 - id_length
                alert_id = func.substring(from_column_text, index_, id_length)
                conditions.append(alert_id == expected_value)

        try:
            link_query = self.session.query(*columns).filter(*conditions).all()
        except Exception:
            logging.exception("Failed to execute query. Retrying after rollback.")
            self.session.rollback()
            link_query = self.session.query(*columns).filter(*conditions).all()

        return_data = [{
            "type": data["type"],
            "original_form": from_form_name_,
            "raw_data": base_form_value[1],
            "link_data": {link["name"]: link_data}
        } for base_form_value in link_query]
        return return_data

    def _get_to_links(self, data):
        link_data = {}
        for link in self.links_by_type.get(data["type"], []):
            to_form = model.form_tables(
                param_config=self.config)[link["to_form"]]
            if link.get("from_condition"):
                column, expected = link["from_condition"].split(":")
                if data.get(column) != expected:
                    continue
            columns = [to_form.uuid, to_form.data]
            conditions = []
            from_columns = link["from_column"].split(";")
            to_columns = link["to_column"].split(";")
            methods = link["method"].split(";")
            for from_column, to_column, method in zip(from_columns, to_columns, methods):
                try:
                    expected = data["raw_data"][from_column]
                    to_column_text = to_form.data[to_column].astext
                except Exception:
                    logging.error(f'ERROR: {data["raw_data"]}')
                    continue
                if method == "match":
                    conditions.append(to_column_text == expected)
                elif method == "lower_match":
                    left = func.replace(func.lower(to_column_text), "-", "_")
                    right = str(expected).lower().replace("-", "_")
                    conditions.append(left == right)
                elif method == "alert_match":
                    alert_id_ = expected[-self.config.country_config["alert_id_length"]:]
                    conditions.append(to_column_text == alert_id_)
                conditions.append(to_column_text != '')

            # handle the filter condition
            if link.get("to_condition"):
                column, expected = link["to_condition"].split(":")
                condition = to_form.data[column].astext == expected
                conditions.append(condition)
            try:
                link_query = self.session.query(*columns).filter(*conditions).all()
            except Exception:
                logging.exception("Failed to execute query. Retrying after rollback.")
                self.session.rollback()
                link_query = self.session.query(*columns).filter(*conditions).all()

            if link["name"] in data.get("link_data", {}):
                link_query.append((None, data["link_data"][link["name"]]))
            if len(link_query) > 1:
                # Want to correctly order the linked forms
                column, method = link["order_by"].split(";")
                if method == "date":
                    sort_function = lambda x: parse(x[1][column])
                else:
                    sort_function = lambda x: x[1][column]
                link_query = sorted(link_query, key=sort_function)
            if link_query:
                link_data[link["name"]] = [link[1] for link in link_query]

        data["link_data"] = link_data
        return data
