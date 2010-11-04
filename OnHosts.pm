package MogileFS::ReplicationPolicy::OnHosts;
use strict;
use base 'MogileFS::ReplicationPolicy';
use MogileFS::Util qw(weighted_list);
use MogileFS::ReplicationRequest qw(ALL_GOOD TOO_GOOD TEMP_NO_ANSWER);

sub new {
    my ($class, @hosts) = @_;
    return bless {
        hosts => @hosts,
    }, $class;
}

sub new_from_policy_args {
    my ($class, $argref) = @_;

    $$argref =~ /\((.*?)\)/oi;
    my @hosts = split ',', $1;

    if (!@hosts) {
        warn "No hosts found in params to OnHosts()";
    }

    my %hostids;
    foreach my $hostname (@hosts) {
        my $host = MogileFS::Host->of_hostname($hostname);
        if (defined $host) {
            $hostids{$host->hostid} = $host;
        }
    }
    
    return $class->new(\%hostids)
}

sub replicate_to {
    my ($self, %args) = @_;

    my $fid      = delete $args{fid};      # fid scalar to copy
    my $on_devs  = delete $args{on_devs};  # arrayref of device objects
    my $all_devs = delete $args{all_devs}; # hashref of { devid => MogileFS::Device }
    my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round
    my $min      = delete $args{min};
    my $already_on = @$on_devs;

    # See which and how many unique hosts we're already on.
    my %on_host;
    my %target_hosts;
    foreach my $dev (@$on_devs) {
        if (defined $self->{hosts}{$dev->hostid}) {
            $target_hosts{$dev->hostid} = 1;
        }

        $on_host{$dev->hostid} = 1;
    }

    # Check we meet the replica count
    my $uniq_targets_on = scalar keys %target_hosts;
    my $uniq_hosts_on   = scalar keys %on_host;

    # We want to have $min copies on targetted nodes, and $min on non-targetted nodes
    return TOO_GOOD if $uniq_targets_on >   $min;
    return TOO_GOOD if $uniq_hosts_on   >   ($min * 2);
    return TOO_GOOD if $uniq_targets_on ==  $min && $already_on > ($min * 2);
    return ALL_GOOD if $uniq_targets_on ==  $min && $uniq_hosts_on == ($min * 2);

    # Replicate normally, avoiding target nodes
    my @all_dests = weighted_list map {
        [$_, 100 * $_->percent_free]
    } grep {
        ! $on_host{$_->hostid}          && # We don't want to hit a host we're already on
        ! $self->{hosts}{$_->hostid}    && # Or a host that is targetted
        ! $failed->{$_->devid}          &&
        $_->should_get_replicated_files
    } values %$all_devs;

    # If we're not on a target node, push them to the top
    if ($uniq_targets_on < $min) {
        my @target_dests = weighted_list map {
            [$_, 100 * $_->percent_free]
        } grep {
            ! $on_host{$_->hostid}      && # We don't want to hit a host we're already on
            $self->{hosts}{$_->hostid}  && # But we only want hosts we're targetting
            ! $failed->{$_->devid}      &&
            $_->should_get_replicated_files
        } values %$all_devs;

        unshift @all_dests, @target_dests;
    }
    
    return TEMP_NO_ANSWER unless @all_dests;

    return MogileFS::ReplicationRequest->new(
        ideal => \@all_dests,
        desperate => (),
    );
}

1;
