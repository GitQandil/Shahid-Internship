from utilities.utils import *
import pandas as pd


def test_convert():
    test = pd.DataFrame({'conv': ['[{\'test1\':\'2\'}]', '[]', '{}']})
    expect = pd.DataFrame({'conv': [[{'test1': '2'}], [], {}]})

    test.conv = convert(test.conv)
    pd.testing.assert_frame_equal(test, expect)


def test_convert_type():

    test = pd.DataFrame({'conv': ['[{\'test1\':\'2\'}]', '[]', '{}']})

    assert isinstance(convert(test.conv)[0], list)


def test_na_ld_obj():
    test = pd.DataFrame({'conv': ['[{\'test1\':\'2\'}]', '[]', '{}']})
    expect = pd.DataFrame({'conv': ['[{\'test1\':\'2\'}]', None, None]})

    result_df = na_ld(test, ['conv'])
    pd.testing.assert_frame_equal(result_df, expect)


def test_na_ld_numeric():
    test = pd.DataFrame({'conv': [1, 2, 0]})
    expect = pd.DataFrame({'conv': [1, 2, None]})

    result_df = na_ld(test, ['conv'])
    pd.testing.assert_frame_equal(result_df, expect)


def test_categorize_gender():
    test = pd.DataFrame({'conv': [1.0, 2.0, 0.0, 1.0, 2.0]})
    expect = pd.DataFrame({'conv': ['male', 'female', 'unspecified', 'male', 'female']})

    test['conv'] = categorize_gender(test, 'conv')
    pd.testing.assert_frame_equal(test, expect)


def test_replace_tt():
    test = pd.DataFrame({'conv': ['tt192034']})
    expect = pd.DataFrame({'conv': ['192034']})

    test['conv'] = test['conv'].apply(replace_tt)
    pd.testing.assert_frame_equal(test, expect)


def test_normalize_all():
    test = pd.DataFrame({'conv': [
        '[{\'test1\':\'1\', \'test2\':\'2\', \'test3\':\'3\'},{\'test1\':\'1\', \'test2\':\'2\', \'test3\':\'3\'}]',
        '[{\'test1\':\'4\', \'test2\':\'5\', \'test3\':\'6\'},{\'test1\':\'4\', \'test2\':\'5\', \'test3\':\'6\'}]'],
        'id': [10, 11]})
    expect = pd.DataFrame({'id': [10, 10, 11, 11], 'test1': ['1', '1', '4', '4'], 'test2': ['2', '2', '5', '5'],
                           'test3': ['3', '3', '6', '6']})

    conv = normalize_all(test, 'conv', 'id')
    pd.testing.assert_frame_equal(conv, expect)
