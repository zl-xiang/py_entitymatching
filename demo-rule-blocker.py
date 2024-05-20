# Import py_entitymatching package
import py_entitymatching as em
import os
import pandas as pd
from pandas import DataFrame



def sim_rule_blocker(tbl1:tuple,tbl2:tuple,result_file='',block_attrs:list=[]):
    A = em.read_csv_metadata(tbl1[0], key=tbl1[1])
    B = em.read_csv_metadata(tbl2[0], key=tbl2[1])
    
    block_t = em.get_tokenizers_for_blocking()
    block_s = em.get_sim_funs_for_blocking()
    #print(block_t)
    atypes1 = em.get_attr_types(A)
    atypes2 = em.get_attr_types(B)
    print(atypes1)
    print(atypes2)
    block_c = em.get_attr_corres(A, B)
    block_f = em.get_features(A, B, atypes1, atypes2, block_c, block_t, block_s)
    #print(  em._block_t )
    #feature = {'feature_name':'venue_venue_jaro_winkler_w', 'left_attribute':'venue', 'right_attribute':'venue', 'left_attr_tokenizer':'wspace', 'right_attr_tokenizer':'wspace', 'simfunction':'jaro_winkler', 'function':em.jaro_winkler, 'function_source':em.tok_wspace, 'is_auto_generated':False}
    #new_row = pd.DataFrame([feature])
    #block_f = pd.concat([block_f,new_row], ignore_index=True)
    #block_f.add()
    
    print(block_f)
    #r = em.get_feature_fn('jaro_winkler(wspace(ltuple["title"])), jaro_winkler(wspace(rtuple["title"]))', block_t, block_s)
   # em.add_feature(block_f,'title_title_jaro_w',r)
    # check attribute correspondance
    # print(em._block_c['corres'])
    # em._atypes1['title'], em._atypes1['authors'], em._atypes1['venue'], em._atypes1['year']
    rb = em.RuleBasedBlocker()
    # Add rule : block tuples if name_name_lev(ltuple, rtuple) < 0.4
    rb.add_rule(['title_title_jac_qgm_3_qgm_3(ltuple, rtuple) < 0.4'], block_f)
    C = rb.block_tables(A, B, l_output_attrs=[tbl1[1],'title','authors','venue','year'], r_output_attrs=[tbl2[1],'title','authors','venue','year'], show_progress=True)
    #print(C)
    #C.to_csv(result_file, encoding='utf-8', index=False)
    
    
if __name__ == "__main__":
    dataset_path = '../dataset/dblp'
    result = f'{dataset_path}/candidates.csv'
    acm = f'{dataset_path}/acm.csv'
    dblp = f'{dataset_path}/dblp.csv'
    tbl1 = (acm,'id')
    tbl2 = (dblp,'id')
    
    sim_rule_blocker(tbl1,tbl2,result)