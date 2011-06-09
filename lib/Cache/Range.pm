package Cache::Range;

use strict;
use warnings;

use Tree::R;
use Storable qw(freeze thaw);

use namespace::clean;

sub new {
    my ( $class, $cache ) = @_;
    
    return bless \$cache, $class;
}

sub set {
    my $self       = shift;
    my $key        = shift;
    my $data_start = shift;
    my $data_end   = shift;
    my $start      = shift;
    my $data       = shift;
    my $cache      = $$self;

    $cache->set(join('_', $key, $data_start, $data_end), freeze({ start_index => $start, data => $data}), @_);
    $data = {
        end   => $data_end,
        start => $data_start,
    };
    my $rtree = $cache->get($key . '_rtree');
    if($rtree) {
        $rtree = thaw($rtree);
    } else {
        $rtree = Tree::R->new;
    }
    $rtree->insert($data, $data_start, 0, $data_end, 0);
    $cache->set($key . '_rtree', freeze($rtree), $Cache::EXPIRES_NEVER);
}

sub get {
    my ( $self, $key, $req_start, $req_end ) = @_;

    my $cache = $$self;
    my $rtree = $cache->get($key . '_rtree');
    my $dirty = 0;

    return unless $rtree;
    $rtree = thaw($rtree);

    my @results;
    my @retval;
    $rtree->query_partly_within_rect($req_start, 0, $req_end, 0, \@results);
    @results = sort { $a->{'start'} <=> $b->{'start'} } @results;

    foreach my $entry (@results) {
        my ( $int_end, $int_start ) = @{$entry}{qw/end start/};
        my $data = $cache->get(join('_', $key, $int_start, $int_end));

        unless($data) {
            $rtree->remove($entry);
            $dirty = 1;
            next;
        }
        $data     = thaw($data);
        my $start = $data->{'start_index'};
        $data     = $data->{'data'};
        my $end   = $start + $#$data;

        if($int_start < $req_start) {
            $int_start = $req_start;
        }
        if($int_end > $req_end) {
            $int_end = $req_end;
        }
        if($start < $req_start) {
            splice @$data, 0, $req_start - $start;
            $start = $req_start;
        }
        if($end > $req_end) {
            splice @$data, $req_end - $end;
        }

        push @retval, [ $int_start, $int_end, $start, $data ];
    }
    if($dirty) {
        $cache->set($key . '_rtree', freeze($rtree), $Cache::EXPIRES_NEVER);
    }
    return @retval;
}

1;

__END__

# ABSTRACT: Caches entries that are associated with an interval in a dataset

=head1 SYNOPSIS

  use Cache::Range;

  my $cache  = Cache::Memory->new; # or any other Cache impl
  my $rcache = Cache::Range->new($cache);
  my $rows = [ 0..99 ];
  $rcache->set($key, 0, 99, 0, $rows, '5 minutes'); # the end of the range is taken
                                                    # from the length of the value
  $rcache->set($key, 110, 209, 110, $rows, '5 minutes');
  my @entries = $rcache->get($key, 50, 90);

  foreach my $entry (@entries) {
    my ( $interval_start, $interval_end, $start_index, $data ) = @$entry;
    # $interval_start should be 50, $interval_end should be 90,
    # $start_index should be 50, and $data will contain rows 50-90
  }

=head1 DESCRIPTION

This utility module builds off of a cache implementation to store data that
are associated with an interval.  For example, say you're querying a database
for ranges of entries, and one query fetches the first one hundred rows.
If on your next query you want rows twenty five through fifty back, the
cache will give them to you, because those rows are contained within an
interval you've already stored.

=head1 METHODS

=head2 Cache::Range->new($cache)

Creates a new Cache::Range object, which stores its entries in C<$cache>.

=head2 $rcache->set($key, $interval_start, $interval_end, $start_index, $value, $expiry)

Stores entries under C<$key> which correspond to the interval C<$interval_start> - C<$interval_end>,
with an optional expiry time.  C<$start_index> is I<usually> the same as C<$interval_start>, which may
seem redundant; C<$start_index> differs in cases where the cached data is a subrange of interval.  For
example, let's say you have an application that inserts web server usage information into a database once an hour,
starting at 8 AM and ending at 8 PM.  Now let's say a user asks a web frontend to the database for usage information
from 6 AM to 10 PM.  Your SQL query will look something like this:

  SELECT n_hits, hour FROM web_server_usage WHERE hour BETWEEN 6 AND 22

You'll get all of the results from 8 AM to 8 PM, but you need to tell the cache that those are the results from
6 AM to 10 PM.  So you call the C<set> method like this:

  $rcache->set($key, 6, 22, 8, $data, '1 hour'); # you'd probably find the value of 8 by scanning the dataset

NOTE: Because this module stores some internal state in the
cache itself, I wouldn't recommend messing around with any cache entries prefixed by C<$key>.

=head2 $rcache->get($key, $start, $end)

Returns a list of array references, each representing a cached range that overlaps C<$start> - C<$end>.
Each array reference contains the interval start, interval end, start index, and value associated with
the cached range (ie. the four arguments after the key to L<""/set>.).

=head1 SEE ALSO

L<Cache>

=cut
