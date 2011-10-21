package Search::OpenSearch::Engine::Xapian;
use warnings;
use strict;
use base 'Search::OpenSearch::Engine';
use SWISH::Prog::Xapian::Searcher;
use SWISH::Prog::Xapian::Indexer;
use SWISH::Prog::Xapian::InvIndex;
use SWISH::Prog::Doc;
use SWISH::Prog::Aggregator;
use SWISH::3 qw( :constants );
use Carp;
use Data::Dump qw( dump );
use Scalar::Util qw( blessed );

our $VERSION = '0.01';

=head1 NAME

Search::OpenSearch::Engine::Xapian - Xapian engine for OpenSearch results

=head1 SYNOPSIS

 use Search::OpenSearch::Engine::Xapian;
 my $engine = Search::OpenSearch::Engine::Xapian->new(
    index       => [qw( path/to/index1 path/to/index2 )],
    facets      => {
        names       => [qw( color size flavor )],
        sample_size => 10_000,
    },
    fields      => [qw( color size flavor )],   # result attributes in response
    indexer_config  => {
        somekey => somevalue,
    },
    searcher_config => {
        anotherkey => anothervalue,
    },
    cache           => CHI->new(
        driver           => 'File',
        dir_create_mode  => 0770,
        file_create_mode => 0660,
        root_dir         => "/tmp/opensearch_cache",
    ),
    cache_ttl       => 3600,
    do_not_hilite   => [qw( color )],
    snipper_config  => { as_sentences => 1 },        # see Search::Tools::Snipper
    hiliter_config  => { class => 'h', tag => 'b' }, # see Search::Tools::HiLiter
    parser_config   => {},                           # see Search::Query::Parser
    
 );
 my $response = $engine->search(
    q           => 'quick brown fox',   # query
    s           => 'rank desc',         # sort order
    o           => 0,                   # offset
    p           => 25,                  # page size
    h           => 1,                   # highlight query terms in results
    c           => 0,                   # count total only (same as f=0 r=0)
    L           => 'field|low|high',    # limit results to inclusive range
    f           => 1,                   # include facets
    r           => 1,                   # include results
    t           => 'XML',               # or JSON
    L           => 'http://yourdomain.foo/opensearch/',
    b           => 'AND',               # or OR
 );
 print $response;

=head1 DESCRIPTION

=head1 METHODS

=cut

sub init_searcher {
    my $self     = shift;
    my $index    = $self->index or croak "index not defined";
    my $searcher = SWISH::Prog::Xapian::Searcher->new(
        invindex => $index,
        debug    => $self->debug,
        %{ $self->searcher_config },
    );
    if ( !$self->fields ) {

        #$self->fields( $searcher->get_propnames );
        croak "TODO get default fields";
    }
    return $searcher;
}

sub init_indexer {
    my $self = shift;

    # unlike a Searcher, which has an array of invindex objects,
    # the Indexer wants only one. We take the first by default,
    # but a subclass could do more subtle logic here.

    my $indexer = SWISH::Prog::Xapian::Indexer->new(
        invindex => $self->index->[0],
        debug    => $self->debug,
        %{ $self->indexer_config },
    );
    return $indexer;
}

sub has_rest_api {1}

sub _massage_rest_req_into_doc {
    my ( $self, $req ) = @_;

    #dump $req;
    my $doc;

    if ( !blessed($req) ) {
        $doc = SWISH::Prog::Doc->new(
            version => 3,
            %$req
        );
    }
    else {

        #dump $req->headers;

        # $req should act like a HTTP::Request object.
        my %args = (
            version => 3,
            url     => $req->uri->path,        # TODO test
            content => $req->content,
            size    => $req->content_length,
            type    => $req->content_type,

            # type
            # action
            # parser
            # modtime
        );

        #dump \%args;

        $doc = SWISH::Prog::Doc->new(%args);

    }

    # use set_parser_from_type so that SWISH::3 does the Right Thing
    # instead of looking at the original mime-type.
    my $aggregator
        = SWISH::Prog::Aggregator->new( set_parser_from_type => 1 );
    $aggregator->swish_filter($doc);

    return $doc;
}

# PUT only if it does not yet exist
sub PUT {
    my $self = shift;
    my $req  = shift or croak "request required";
    my $doc  = $self->_massage_rest_req_into_doc($req);
    my $uri  = $doc->url;

    # edge case: index might not yet exist.
    my $exists;
    my $indexer = $self->init_indexer();
    if ( -s $indexer->invindex->path->file('swish.xml') ) {
        $exists = $self->GET($uri);
        if ( $exists->{code} == 200 ) {
            return { code => 409, msg => "Document $uri already exists" };
        }
    }
    $indexer->process($doc);
    my $total = $indexer->finish();
    $exists = $self->GET( $doc->url );
    if ( $exists->{code} != 200 ) {
        return { code => 500, msg => 'Failed to PUT doc' };
    }
    return { code => 201, total => $total, doc => $exists->{doc} };
}

# POST allows new and updates
sub POST {
    my $self    = shift;
    my $req     = shift or croak "request required";
    my $doc     = $self->_massage_rest_req_into_doc($req);
    my $uri     = $doc->url;
    my $indexer = $self->init_indexer();
    $indexer->process($doc);
    my $total  = $indexer->finish();
    my $exists = $self->GET( $doc->url );

    if ( $exists->{code} != 200 ) {
        return { code => 500, msg => 'Failed to POST doc' };
    }
    return { code => 200, total => $total, doc => $exists->{doc} };
}

sub DELETE {
    my $self     = shift;
    my $uri      = shift or croak "uri required";
    my $existing = $self->GET($uri);
    if ( $existing->{code} != 200 ) {
        return {
            code => 404,
            msg  => "$uri cannot be deleted because it does not exist"
        };
    }
    my $indexer = $self->init_indexer();
    $indexer->get_lucy->delete_by_term(
        field => 'swishdocpath',
        term  => $uri,
    );
    $indexer->finish();
    return {
        code => 204,    # no content in response
    };
}

sub GET {
    my $self = shift;
    my $uri = shift or croak "uri required";

    # TODO get by term
    my $x_uri    = join( '', SWISH_PREFIX_URL(), $uri );
    my $searcher = $self->searcher();
    my $results  = $searcher->search( $x_uri, { max => 1 } );
    my $hit      = $results->next;
    if ( !$hit ) {
        return { code => 404, };
    }

    # get all fields
    my %doc;
    my $fields = $self->fields;
    for my $field (@$fields) {
        my $str = $hit->get_property($field);
        $doc{$field} = [ split( m/\003/, defined $str ? $str : "" ) ];
    }
    $doc{title}   = $hit->title;
    $doc{summary} = $hit->get_property('swishdescription');

    my $ret = {
        code => 200,
        doc  => \%doc,
    };

    #dump $ret;

    return $ret;
}

1;

__END__

=head1 AUTHOR

Peter Karman, C<< <karman at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-search-opensearch-engine-xapian at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Search-OpenSearch-Engine-Xapian>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Search::OpenSearch::Engine::Xapian


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Search-OpenSearch-Engine-Xapian>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Search-OpenSearch-Engine-Xapian>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Search-OpenSearch-Engine-Xapian>

=item * Search CPAN

L<http://search.cpan.org/dist/Search-OpenSearch-Engine-Xapian/>

=back

=head1 COPYRIGHT & LICENSE

Copyright 2011 Peter Karman.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

