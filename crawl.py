#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import datetime
import requests
import argparse



def parse(path):

    from bs4 import BeautifulSoup

    nodes = {}
    edges = []
    
    xml = BeautifulSoup(open(path, 'r').read() )
    for n in xml.gexf.graph.findAll("node"):
        id, label = n['id'],n.get('label', "unknown")
        for e in n.attvalues.findAll('attvalue'):
            if e['for'] == "tags" : tags = e["value"].split(",")
            if e['for'] == "urls" : url = e["value"]
        nodes[id] = {
                    "label" : label,
                    "url" : url,
                    "tags" : tags,
                    "pid" : id,
                }

    # <edge id="53083" source="d1bb01d2-34b7-415b-b887-146370b8b3d7" target="65d5e63b-ed13-4c0c-a8f5-27d40b8fedd2"></edge>
    for e in xml.gexf.graph.findAll("edge"):
        edges.append( {
            "source": e['source'],
            "target": e['target'],
        } )
    
    print "parsed", path, len(nodes), len(edges)
    return nodes, edges

    
def to_padagraph(host, key, gid, path):
    from reliure.types import Text, Numeric 
    from botapi import Botagraph, BotApiError
    
    bot = Botagraph(host, key)

    nodes, edges = parse(path)
    
    if not bot.has_graph(gid) :
        
        print "\n * Create graph %s" % gid
        attrs = {
            'description':
            """
            http://utopies-concretes.org/#/fr
            
            Ils ont essayé de nous enterrer, ils ne savaient pas que nous étions des graines.

            Un graphe de près de 3000 sites internet de collectifs, structures, médias, blogs — positions relatives et interconnexions
            """.replace("    ", ""),
    
            'image': "",
            'tags': ['social-network', 'utopies-concretes']
        }

        print "\n * Creating graph %s" % gid
        
        bot.create_graph(gid, attrs )
                        
        print "\n * Creating node type %s" % ""
        props = {
                    'label' : Text(),
                    'url'  : Text(),
                    'tags' : Text(multi=True, uniq=True),
                    'image' : Text(),
                    'description' : Text()
                }
        bot.post_nodetype(gid, "Site",  "Site ", props)

        print "\n * Creating edge type %s" % "follows"
        props = {
                    'score' : Numeric(),
                }
        bot.post_edgetype(gid, "is_related", "is_related", props )
    

    schema = bot.get_schema(gid)['schema']
    nodetypes = { n['name']:n for n in schema['nodetypes'] }
    edgetypes = { e['name']:e for e in schema['edgetypes'] }

    def gen_nodes():
        for k,v in nodes.iteritems():     
            
            yield {
                'nodetype': nodetypes['Site']['uuid'],
                'properties': v
            }
    
    print "posting nodes"
    count = 0
    fail = 0
    idx = {}
    for node, uuid in bot.post_nodes( gid, gen_nodes() ):
        if not uuid:
            fail += 1
        else :
            count += 1
            idx[node['properties']['pid']] = uuid
        
    print "%s nodes inserted " % count

    
    def gen_edges():
        for e in edges: 

            src = idx.get(e["source"], None)
            tgt = idx.get(e["target"], None)
            if src and tgt:
                yield {
                    'edgetype': edgetypes['is_related']['uuid'],
                    'source': src,
                    'label' : "is_related",
                    'target': tgt,
                    'properties': {'score':1}
                }

    print "posting edges"
    count = fail = 0

    for obj, uuid in bot.post_edges( gid, gen_edges() ):
        if not uuid:
            fail += 1
        else :
            count += 1
    print "%s edges inserted " % count


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--host", action='store', help="host", default=None)
    parser.add_argument("--key", action='store', help="host", default=None)
    parser.add_argument("--gid", action='store', help="host", default="")
    parser.add_argument("--path", action='store', help="gexf file path", default=False)
    
    args = parser.parse_args()

        
    if args.host and args.key and args.gid:
        if args.path:
            to_padagraph(args.host, args.key, args.gid, args.path)
        return


if __name__ == '__main__':
    sys.exit(main())