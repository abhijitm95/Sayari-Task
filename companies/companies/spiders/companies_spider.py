# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 01:35:07 2020

@author: Abhijit Menon
"""

import scrapy
import pandas as pd
import json
import networkx as nx
import matplotlib.pyplot as plt

#I created 2 spiders. The first one is to get the name of all active companies starting with X.
class companies_spider(scrapy.Spider):
    name="companies"
    def start_requests(self):
        #specify headers to parse json content from POST request.
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
            "Host":"firststop.sos.nd.gov",
            "Origin":"https://firststop.sos.nd.gov",
            }
        #add serach specifications (starting with an X and currently active)
        payload={'SEARCH_VALUE': "X", 
                 'STARTS_WITH_YN': "true", 
                 'ACTIVE_ONLY_YN': "true"}
        
        urls="https://firststop.sos.nd.gov/api/Records/businesssearch"
        
        yield scrapy.Request(url=urls,
                             method="POST",
                             headers=headers,
                             body=json.dumps(payload),
                             callback=self.parse)
    
    def parse(self,response):
        data= json.loads(response.body) #get a nested json containing info
        ids=list(data['rows'].keys()) #get ids stored as keys from json
        json_values=list(data['rows'].values()) #get values from json
        names_list=[i['TITLE'][0] for i in json_values] #get names of company stored as first element of list in the value with key "TITLE"
        df=pd.DataFrame(list(zip(ids,names_list)),columns=['ID','Company_Name']) #create df with id and names
        df.to_csv('company_info.csv',mode='a') #save df to a csv

def create_urls(df,url):
    """
    Editing the base url with the IDs obtained from the ID column
    of the company dataframe, retuning a list with the completed urls for the
    scraper.
    
    Args:
        df (DataFrame): The dataframe with company info (ID, Company Name)
        url (str): The base url with {} inplace of ID.
    
    Returns:
        List of urls
    """
    ids=df['ID']
    links=[url.format(i) for i in ids] #using string formatting to replace {} in base url with the ID.
    return links

#Spider to get owner and agent info of all the companies.
class owner_spider(scrapy.Spider):
    name="owner_agent"
    def start_requests(self):
        df=pd.read_csv('company_info.csv')
        base_url="https://firststop.sos.nd.gov/api/FilingDetail/business/{}/false"
        urls=create_urls(df,base_url) #call create_urls from above
        for i in range(len(urls)): #iterate through all the links
            yield scrapy.Request(url=urls[i],
                             method="GET",
                             callback=self.parse)

    def parse(self,response):
        data= response.text
        #using string split function to get data out from the response text
        try:
            agent=data.split("Registered Agent</LABEL><VALUE>")[1].split("</VALUE><ALERT_YN>")[0]
        except:
            agent="Not found"
        try:
            owner=data.split("Owner Name</LABEL><VALUE>")[1].split("</VALUE><ALERT_YN>")[0]
        except:
            try:
                owner=data.split("Owners</LABEL><VALUE>")[1].split("</VALUE><ALERT_YN>")[0]
            except:         
                owner="Not Found"
        info_dic={"url":response.url,"company_agent":[agent],"company_owner":[owner]}
        df=pd.DataFrame(info_dic)
        df.to_csv('owner_agent.csv',mode='a',header=False)

def final_file(path_name_file,path_owner_agent_file):
    """
    Merge 2 dataframes to create a final dataframe with all information for 
    each company.
    
    Args:
        path_name_file (str): Directory path to file containing names and ID 
                              created by the companies spider.
        path_owner_agent_file (str): Directory path to file containing owner
                                     and agent information created by the 
                                     owner_agent spider.
                                     
    Returns:
        DataFrame
    """
    names=pd.read_csv(path_name_file,usecols=[1,2]) #read first file
    owner_agent=pd.read_csv(path_owner_agent_file,usecols=[1,2,3],names=['url','agent','owner']) #read second file
    owner_agent['ID']=owner_agent.apply(lambda x:(x['url'].split('business/')[1].split('/false')[0]),axis=1)#extract id from url
    owner_agent['ID']=owner_agent['ID'].astype(int) #convert str to int
    owner_agent['Agent']=owner_agent.apply(lambda x:(x['agent'].split('\r')[0]),axis=1) #extract agent name and remove address
    owner_agent.drop(columns=['agent'],inplace=True)
    owner_agent['Owner']=owner_agent.apply(lambda x:(x['owner'].split('\r')[0]),axis=1) #extract owner name and remove address
    owner_agent.drop(columns=['owner'],inplace=True)

    final_df=names.merge(owner_agent,left_on='ID',right_on='ID') #merge the 2 files to create the final crawled file.
    return final_df

def graph_plot(graph_df):
    """
    Plotting networkX graphs for Company->Agents and Company->Owner
    relationships.
    
    Args:
        graph_df(DataFrame):A dataframe containing information for all 
                            companies.
    
    Returns:
        A graph plot.
    """
    agent_graph=graph_df[graph_df['Agent']!='Not found'] #Filtering companies who did not have an registered agent
    agents=list(agent_graph.Agent.unique()) #generate list of unique agents
    companies=list(agent_graph.Company_Name.unique()) #generate list of unique companies
    plt.figure(figsize=(20, 20))
    G=nx.from_pandas_edgelist(agent_graph,'Company_Name','Agent')
    layout=nx.spring_layout(G,iterations=16) #use spring layout for plot
    agent_size=[G.degree(agent)*100 for agent in agents]
    
    nx.draw_networkx_nodes(G, layout, nodelist=agents, node_size=agent_size, 
                           node_color='b') #specify 

    nx.draw_networkx_nodes(G, layout, nodelist=companies, node_color='r', 
                           node_size=100)

    nx.draw_networkx_edges(G, layout, edge_color="gray")    
    agent_dict = dict(zip(agents, agents))
    nx.draw_networkx_labels(G, layout, labels=agent_dict)


    owner_graph=graph_df[graph_df['Owner']!='Not Found'] #Filtering companies who did not have an owner
    owners=list(owner_graph.Owner.unique()) #generate list of unique owners
    companies=list(owner_graph.Company_Name.unique()) #generate list of unique companies
    H=nx.from_pandas_edgelist(owner_graph,'Company_Name','Owner')
    layout=nx.spring_layout(H,iterations=16) #use spring layout for plot
    owner_size=[H.degree(owner)*100 for owner in owners]
    
    nx.draw_networkx_nodes(H, layout, nodelist=owners, node_size=owner_size,
                           node_color='g')
    nx.draw_networkx_nodes(H, layout, nodelist=companies, node_color='r', 
                           node_size=100)
    
    nx.draw_networkx_edges(H, layout, edge_color="gray")
    owner_dict = dict(zip(owners, owners))
    nx.draw_networkx_labels(G, layout, labels=owner_dict)
    plt.axis('off')
    plt.show()
    plt.savefig('graph_plot.png')

graph_df=final_file('D:/Python/Sayari/companies/companies/company_info.csv',
                          'D:/Python/Sayari/companies/companies/owner_agent.csv') #call function using the paths to the files created by the companies and owner_agent spider respectively.

graph_df.to_csv('company_info_crawled.csv') #save all date to csv file

graph_plot(graph_df) #plot networkX plot


