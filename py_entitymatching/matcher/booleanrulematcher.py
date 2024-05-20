"""
This file contains functions for boolean rule based matcher.
# NOTE: This will not be included in the first version of py_entitymatching release
"""
from collections import OrderedDict
import logging

import py_entitymatching.catalog.catalog_manager as cm
from py_entitymatching.matcher.rulematcher import RuleMatcher
from py_entitymatching.matcher.matcherutils import get_ts
from py_entitymatching.utils.validation_helper import validate_object_type
from py_entitymatching.utils.generic_helper import parse_conjunct
import pandas as pd
import six

logger = logging.getLogger(__name__)

class BooleanRuleMatcher(RuleMatcher):
    def __init__(self, *args, **kwargs):
        name = kwargs.pop('name', None)
        if name is None:
            self.name = 'BooleanRuleMatcher' + '_' + get_ts()
        else:
            self.name = name
        self.rules = OrderedDict()
        self.rule_source = OrderedDict()
        self.rule_conjunct_list = OrderedDict()
        self.rule_cnt = 0
        feature_table = kwargs.pop('feature_table', None)
        self.feature_table = feature_table
        self.rule_ft = OrderedDict()

    def fit(self):
        pass

    def _predict_candset(self, candset, verbose=False):
        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(candset, logger, verbose)

        # # validate metadata
        cm._validate_metadata_for_candset(candset, key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key,
                                          logger, verbose)

        # # keep track of predictions
        predictions = []

        # # set index for convenience
        l_df = ltable.set_index(l_key, drop=False)
        r_df = rtable.set_index(r_key, drop=False)

        # # get the index of fk_ltable and fk_rtable from the cand. set
        col_names = list(candset.columns)
        lid_idx = col_names.index(fk_ltable)
        rid_idx = col_names.index(fk_rtable)

        # # iterate through the cand. set
        for row in candset.itertuples(index=False):
            l_row = l_df.loc[row[lid_idx]]
            r_row = r_df.loc[row[rid_idx]]
            res = self.apply_rules(l_row, r_row)
            if res is True:
                predictions.append(1)
            else:
                predictions.append(0)

        return predictions

    def predict(self, table=None, target_attr=None, append=False, inplace=True):
        """Predict interface for the matcher.

            A point to note is all the input parameters have a default value of
            None.

            Args:
                table (DataFrame): The input candidate set of type pandas DataFrame
                    containing tuple pairs (defaults to None).
                target_attr (string): The attribute name where the predictions
                    need to be stored in the input table (defaults to None).
                append (boolean): A flag to indicate whether the predictions need
                    to be appended in the input DataFrame (defaults to False).
                return_probs (boolean): A flag to indicate where the prediction probabilities
                    need to be returned (defaults to False). If set to True, returns the
                    probability if the pair was a match.
                inplace (boolean): A flag to indicate whether the append needs to be
                    done inplace (defaults to True).

            Returns:
                An array of predictions or a DataFrame with predictions updated.

            Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
                >>> brm.add_rule(rule, match_f)
                >>> # The table S is a cand set generated by the blocking and then labeling phases
                >>> brm.predict(S, target_attr='pred_label', append=True)

        """

        # Validate input parameters
        # # We expect the table to be of type pandas DataFrame
        validate_object_type(table, pd.DataFrame, 'Input table')

        # # We expect the target_attr to be of type string if not None
        if target_attr is not None and not isinstance(target_attr, str):
                logger.error('Input target_attr must be a string.')
                raise AssertionError('Input target_attr must be a string.')

        # # We expect the append to be of type boolean
        validate_object_type(append, bool, 'Input append')

        # # We expect the inplace to be of type boolean
        validate_object_type(inplace, bool, 'Input inplace')

        # # get metadata
        key, fk_ltable, fk_rtable, ltable, rtable, l_key, r_key = cm.get_metadata_for_candset(
            table, logger, False)

        # # validate metadata
        cm._validate_metadata_for_candset(table, key, fk_ltable, fk_rtable,
                                          ltable, rtable, l_key, r_key,
                                          logger, False)

        # Validate that there are some rules
        assert len(self.rules.keys()) > 0, 'There are no rules to apply'

        # Parse conjuncts to validate that the features are in the feature table
        for rule in self.rule_conjunct_list:
            for conjunct in self.rule_conjunct_list[rule]:
                parse_conjunct(conjunct, self.rule_ft[rule])

        if table is not None:
            y = self._predict_candset(table)
            if target_attr is not None and append is True:
                if inplace == True:
                    table[target_attr] = y
                    return table
                else:
                    tbl = table.copy()
                    tbl[target_attr] = y
                    return tbl
            else:
                return y
        else:
            raise SyntaxError('The arguments supplied does not match the signatures supported !!!')

    def _create_rule(self, conjunct_list, feature_table, rule_name=None):
        # set the name
        if rule_name is None:
            name = '_rule_' + str(self.rule_cnt)
            self.rule_cnt += 1
        else:
            # use the name supplied by the user
            name = rule_name

        fn_str = self.get_function_str(name, conjunct_list)

        if feature_table is not None:
            feat_dict = dict(zip(feature_table['feature_name'], feature_table['function']))
        else:
            feat_dict = dict(zip(self.feature_table['feature_name'], self.feature_table['function']))

        # exec fn_str in feat_dict
        six.exec_(fn_str, feat_dict)
        return feat_dict[name], name, fn_str

    def add_rule(self, conjunct_list, feature_table=None, rule_name=None):
        """Adds a rule to the rule-based matcher.

            Args:
                conjunct_list (list): A list of conjuncts specifying the rule.

                feature_table (DataFrame): A DataFrame containing all the
                                           features that are being referenced by
                                           the rule (defaults to None). If the
                                           feature_table is not supplied here,
                                           then it must have been specified
                                           during the creation of the rule-based
                                           blocker or using set_feature_table
                                           function. Otherwise an AssertionError
                                           will be raised and the rule will not
                                           be added to the rule-based blocker.

                rule_name (string): A string specifying the name of the rule to
                                    be added (defaults to None). If the
                                    rule_name is not specified then a name will
                                    be automatically chosen. If there is already
                                    a rule with the specified rule_name, then
                                    an AssertionError will be raised and the
                                    rule will not be added to the rule-based
                                    blocker.

            Returns:
                The name of the rule added (string).

                Raises:
                    AssertionError: If `rule_name` already exists.

                    AssertionError: If `feature_table` is not a valid value
                         parameter.

            Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
                >>> brm.add_rule(rule, match_f)

            """

        if rule_name is not None and rule_name in self.rules.keys():
            logger.error('A rule with the specified rule_name already exists.')
            raise AssertionError('A rule with the specified rule_name already exists.')

        if feature_table is None and self.feature_table is None:
            logger.error('Either feature table should be given as parameter ' +
                         'or use set_feature_table to set the feature table.')
            raise AssertionError('Either feature table should be given as ' +
                                 'parameter or use set_feature_table to set ' +
                                 'the feature table.')

        if not isinstance(conjunct_list, list):
            conjunct_list = [conjunct_list]

        if feature_table is None:
            feature_table = self.feature_table

        fn, name, fn_str = self._create_rule(conjunct_list, feature_table, rule_name)

        self.rules[name] = fn
        self.rule_source[name] = fn_str
        self.rule_conjunct_list[name] = conjunct_list
        if feature_table is not None:
            self.rule_ft[name] = feature_table
        else:
            self.rule_ft[name] = self.feature_table

        return name

    def delete_rule(self, rule_name):
        """Deletes a rule from the rule-based matcher.

            Args:
               rule_name (string): Name of the rule to be deleted.

            Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
                >>> brm.add_rule(rule, match_f)
                >>> brm.delete_rule('rule_1')

        """

        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'

        del self.rules[rule_name]
        del self.rule_source[rule_name]
        del self.rule_conjunct_list[rule_name]

        return True

    def view_rule(self, rule_name):
        """Prints the source code of the function corresponding to a rule.

            Args:
               rule_name (string): Name of the rule to be viewed.

            Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
                >>> brm.add_rule(rule, match_f)
                >>> brm.view_rule('rule_1')

        """

        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        print(self.rule_source[rule_name])

    def get_rule_names(self):
        """Returns the names of all the rules in the rule-based matcher.

           Returns:
               A list of names of all the rules in the rule-based matcher (list).

           Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
                >>> brm.add_rule(rule, match_f)
                >>> brm.get_rule_names()

        """

        return self.rules.keys()

    def get_rule(self, rule_name):
        """Returns the function corresponding to a rule.

           Args:
               rule_name (string): Name of the rule.

           Returns:
               A function object corresponding to the specified rule.

           Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> rule = ['address_address_lev(ltuple, rtuple) > 6']
                >>> brm.add_rule(rule, match_f)
                >>> brm.get_rule()

        """

        assert rule_name in self.rules.keys(), 'Rule name not in current set of rules'
        return self.rules[rule_name]

    def apply_rules(self, ltuple, rtuple):
        #print(ltuple, '-----')
        #print(rtuple, '+++++')
        for fn in self.rules.values():
            res = fn(ltuple, rtuple)
            if res is True:
                return True
        return False

    def get_function_str(self, name, conjunct_list):
        # create function str
        fn_str = "def " + name + "(ltuple, rtuple):\n"
        # add 4 tabs
        fn_str += '    '
        fn_str += 'return ' + ' and '.join(conjunct_list)
        return fn_str

    def set_feature_table(self, feature_table):
        """Sets feature table for the rule-based matcher.

            Args:
               feature_table (DataFrame): A DataFrame containing features.

            Examples:
                >>> import py_entitymatching as em
                >>> brm = em.BooleanRuleMatcher()
                >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='id')
                >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='id')
                >>> match_f = em.get_features_for_matching(A, B)
                >>> brm.set_feature_table(match_f)
        """

        if self.feature_table is not None:
            logger.warning(
                'Feature table is already set, changing it now will not recompile '
                'existing rules')
        self.feature_table = feature_table
