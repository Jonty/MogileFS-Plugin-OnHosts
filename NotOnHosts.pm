package MogileFS::ReplicationPolicy::NotOnHosts;
use strict;
use base 'MogileFS::ReplicationPolicy';
use MogileFS::Util qw(weighted_list);
use MogileFS::ReplicationRequest qw(ALL_GOOD TOO_GOOD TEMP_NO_ANSWER);

sub new {
    my ($class, @hosts) = @_;
    return bless {
        hosts => @hosts,
        rebalancing => 0,
    }, $class;
}

sub new_from_policy_args {
	my ($class, $argref) = @_;

	$$argref =~ /\((.*?)\)/oi;
	my @hosts = split ',', $1;

	if (!@hosts) {
		warn "No hosts found in params to NotOnHosts()";
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
	my $all_devs = delete $args{all_devs}; # hashref of { devid => MogileFS::Device
	my $failed   = delete $args{failed};   # hashref of { devid => 1 } of failed attempts this round
	my $min      = delete $args{min};

	# number of devices we currently live on
	my $already_on = @$on_devs;

	# Work out what hosts we're on
	my %on_host;
	my %bad_devs;
	foreach my $dev (@$on_devs) {
		$on_host{$dev->hostid} = 1;

		if (defined $self->{hosts}{$dev->hostid}) {
			$bad_devs{$dev->id} = 1;
		}
	}

	# If we have some bad devs and we're not already rebalancing, move the files off the devs
	if (%bad_devs && !$self->{rebalancing}) {
		my $success = 0;

		foreach my $dev (keys %bad_devs) {
			my $devfid = MogileFS::DevFID->new($dev, $fid);

			# rebalance_devfid calls replicate again, so we don't want to infinite loop
			$self->{rebalancing} = 1;

			# A bit dirty, but if we don't re-init the store then the locks
			# will clash when we reenter, and mogile will crash. Sadly there
            # appears to be no way around this without major code surgery.
			Mgd::get_store()->init();
			
			$success = MogileFS::Worker::Replicate->rebalance_devfid($devfid);
		}

		# The rebalance will take care of the distribution
		$self->{rebalancing} = 0;

		# We've just triggered a seperate replication for this file, so let's come
        # back later and see what we need to do.
		return TEMP_NO_ANSWER;
	}

    my $uniq_hosts_on = scalar keys %on_host;
    return TOO_GOOD if $uniq_hosts_on >  $min;
    return TOO_GOOD if $uniq_hosts_on == $min && $already_on > $min;
    return ALL_GOOD if $uniq_hosts_on == $min;

	# If we're rebalancing, return list of nodes we can move the files to
	my @target_devs = weighted_list map {
		[$_, 100 * $_->percent_free]
	} grep {
		! $on_host{$_->hostid}	        && # We don't want to hit a host we're already on
		! $self->{hosts}{$_->hostid}	&& # Or a host that is targetted
		! $failed->{$_->devid}	        &&
		$_->should_get_replicated_files
	} values %$all_devs;
    
	return TEMP_NO_ANSWER unless @target_devs;

	return MogileFS::ReplicationRequest->new(
		ideal => \@target_devs,
		desperate => (),
	);
}

1;
