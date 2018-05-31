from collections import namedtuple
from Ranger import RangeSet
from Ranger import Range
from itertools import chain

IBDSource = namedtuple('IBDSource', ['contig','chr','id'])

class IBDSegment(namedtuple('IBDSegment', ['start', 'end', 'source'])): 
    def to_range(self):
        return Range.closedOpen(self.start, self.end)

class IBDSet:

    @classmethod
    def create_complete(cls, id, contig_and_sizes ):
        return IBDSet([[IBDSegment(0, size, IBDSource(contig, 0, id)) for (contig,size) in contig_and_sizes.items()],
            [IBDSegment(0, size, IBDSource(contig,1, id)) for (contig,size) in contig_and_sizes.items()]])


    @classmethod
    def create_full(cls, id , size = 1000, contig='1'):
        return IBDSet([[IBDSegment(0, size, IBDSource(contig, 0, id))],[IBDSegment(0, size, IBDSource(contig,1, id))]])
    
    
    @classmethod
    def haploid_meiosis(cls, ms, size = 1000, contig = '1'):
        # need to convert split to the set of ranges
        assert(not ms.splits or ms.splits[len(ms.splits)-1] <= size)
        segments = []
        current_source = ms.start_with
        all_ranges = [0] + ms.splits + [size]
        for i in range(0, len(all_ranges) -1):
            segment = IBDSegment(all_ranges[i], all_ranges[i+1], IBDSource(contig, current_source, None))
            if (segment.end > segment.start):
                segments.append(segment) 
            current_source = (current_source +1)%2 
        return segments
        
    @classmethod
    def haploid_zygote(cls, zs, contig_sizes):
        # need to convert split to the set of ranges
        segments = []
        for contig,ms in zs.contig_meiosis.items():
            segments += cls.haploid_meiosis(ms, contig_sizes[contig], contig)
        return segments
    
    @classmethod
    def parental_meiosis(cls, ms, size = 1000):
        return IBDSet([cls.haploid_meiosis(ms,size), []])

        
    # Diploid list of sorted segments
    def __init__(self, disgements):
        self.disgements = disgements

    def __repr__(self):
        return str(self.disgements)
        
    def compose_haplo(self, other):
        # merges given haplo with current
        # go through the ranges in current haplo and combine with corresponding ranges 
        # in this diploids
        result = []
        for i in range(0,2):
            all_sources = set(s.source for s in self.disgements[i])
            for source in all_sources:
                this_set = RangeSet([s.to_range() for s in self.disgements[i] if s.source == source])
                other_set = RangeSet([s.to_range() for s in other if s.source.chr==i and s.source.contig == source.contig])
                result += [IBDSegment(r.lowerEndpoint(), r.upperEndpoint(), source) for r in this_set.intersection(other_set)]
        return sorted(result, key=lambda s:s.start)
        
    def compose(self, other):
        return IBDSet([self.compose_haplo(oh) for oh in other.disgements])
    
    def filter_by_id(self, idd):
        return IBDSet([ filter(lambda s: s.source.id == idd, oh)  for oh in self.disgements ])

    def filter_by_ids(self, ids):
        return IBDSet([ filter(lambda s: s.source.id in ids, oh)  for oh in self.disgements ])

    def get_all_sources(self):
        return set(s.source for s in chain(*self.disgements)) 
    
    def intersect(self, other):
        # get a single list of shared IBD segments
        common_sources = self.get_all_sources() & other.get_all_sources()
        # intersect ibds by source
        result = []
        for source in common_sources:
            this_set = RangeSet([s.to_range() for s in chain(*self.disgements) if s.source == source])
            other_set = RangeSet([s.to_range() for s in chain(*other.disgements) if s.source == source])
            result += [IBDSegment(r.lowerEndpoint(), r.upperEndpoint(), source) for r in this_set.intersection(other_set)]
        return sorted(result, key=lambda s:s.start)

    
class MeiosisSpec:
    def __init__(self, start_with, splits):
        self.start_with = start_with
        self.splits = splits
        
    def __repr__(self):
        return str(self.splits)


        
class ZygoteSpec:
    def __init__(self, contig_meiosis):
        self.contig_meiosis = contig_meiosis
    
    def __repr__(self):
        return "ZygoteSpec:" + str(self.contig_meiosis)

        
class GameteSpec:
    
    @classmethod
    def from_json(cls, jdict, contig_spec):
        """ Loads Gamete spec from a json dict representation
        """
        def to_zygote_spec(jgamete_splits):
            return ZygoteSpec(dict( (contig, MeiosisSpec(jsplit['startWith'],
                                jsplit['crossingOvers'])) for (contig, jsplit) in jgamete_splits.items()))
        return GameteSpec(to_zygote_spec(jdict['offspring']['fatherGamete']['splits']),
                            to_zygote_spec(jdict['offspring']['motherGamete']['splits']),
                            contig_spec)
     
     
    def __init__(self, fathernal, mothernal, contig_spec):
        self.fathernal = fathernal
        self.mothernal = mothernal
        self.contig_spec = contig_spec
        
    def apply_to(self, father_idb, mother_ibd):
        father_idb_meiosis = IBDSet.haploid_zygote(self.fathernal, self.contig_spec)
        mothernal_idb_meiosis = IBDSet.haploid_zygote(self.mothernal, self.contig_spec)
        return IBDSet([father_idb.compose_haplo(father_idb_meiosis),
            mother_ibd.compose_haplo(mothernal_idb_meiosis)
        ])
    
    def __repr__(self):
        return "GameteSpec" + str(self.fathernal) + str(self.mothernal)
     

