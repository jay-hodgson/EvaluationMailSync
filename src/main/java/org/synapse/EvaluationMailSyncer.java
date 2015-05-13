package org.synapse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.TeamMember;
import org.sagebionetworks.repo.model.UserProfile;
import org.sagebionetworks.repo.web.NotFoundException;

import com.ecwid.mailchimp.MailChimpClient;
import com.ecwid.mailchimp.MailChimpException;
import com.ecwid.mailchimp.MailChimpObject;
import com.ecwid.mailchimp.method.v1_3.list.ListBatchSubscribeMethod;
import com.ecwid.mailchimp.method.v1_3.list.ListBatchUnsubscribeMethod;
import com.ecwid.mailchimp.method.v1_3.list.ListMembersMethod;
import com.ecwid.mailchimp.method.v1_3.list.ListMembersResult;
import com.ecwid.mailchimp.method.v1_3.list.MemberStatus;
import com.ecwid.mailchimp.method.v1_3.list.ShortMemberInfo;

/**
 * The worker that processes messages for Evaluation asynchronous jobs.
 * 
 * @author dburdick
 *
 */
public class EvaluationMailSyncer {
	
	static private Log log = LogFactory.getLog(EvaluationMailSyncer.class);
	static private enum CurrentChallenges { AML, BROAD_GE };	
	static private final String OVERALL_DREAM_MAILCHIMP_LIST_ID = "8ef794accf";
	
	String mailChimpApiKey;
	MailChimpClient mailChimpClient;
	SynapseClient synapse;
	Map<CurrentChallenges, String> challengeToMailChimpId;
	Map<CurrentChallenges, List<String>> challengeToApprovedTeamIds;
	Map<CurrentChallenges, String> challengeToUnapprovedMailChimpId;
	Map<CurrentChallenges, String> challengeToAllRegisteredTeamId;
	Map<CurrentChallenges, Set<String>> approvedUserEmails;
	
	public EvaluationMailSyncer(String mailChimpApiKey, String synapseUsername,
			String synapsePassword) throws SynapseException {
		if(mailChimpApiKey == null) throw new IllegalArgumentException("mailChimpApiKey cannot be null");
		if(synapseUsername == null) throw new IllegalArgumentException("synapseUserName cannot be null");
		if(synapsePassword == null) throw new IllegalArgumentException("synapsePassword cannot be null");
		
		this.mailChimpApiKey = mailChimpApiKey;
		this.mailChimpClient = new MailChimpClient();
		this.synapse = createSynapseClient();
		synapse.login(synapseUsername, synapsePassword);
		
		approvedUserEmails = new HashMap<EvaluationMailSyncer.CurrentChallenges, Set<String>>();
		approvedUserEmails.put(CurrentChallenges.AML, new HashSet<String>());
		approvedUserEmails.put(CurrentChallenges.BROAD_GE, new HashSet<String>());

		
		challengeToMailChimpId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToMailChimpId.put(CurrentChallenges.AML, "e709b3bc70");
		challengeToMailChimpId.put(CurrentChallenges.BROAD_GE, "87b4ede397");

		challengeToApprovedTeamIds = new HashMap<EvaluationMailSyncer.CurrentChallenges, List<String>>();
		challengeToApprovedTeamIds.put(CurrentChallenges.AML, Arrays.asList(new String[]{ "3320952" }));
		challengeToApprovedTeamIds.put(CurrentChallenges.BROAD_GE, Arrays.asList(new String[]{ "3320950" }));

		challengeToUnapprovedMailChimpId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToUnapprovedMailChimpId.put(CurrentChallenges.AML, "ab99ff9a9c");
		challengeToUnapprovedMailChimpId.put(CurrentChallenges.BROAD_GE, "ac82946ea5");
		
		challengeToAllRegisteredTeamId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToAllRegisteredTeamId.put(CurrentChallenges.AML, "3320951");
		challengeToAllRegisteredTeamId.put(CurrentChallenges.BROAD_GE, "3320949");
		
	}

	public void sync() throws Exception {				
		for(CurrentChallenges challenge : challengeToMailChimpId.keySet()) {
			try{
				// Sync all approved challenges
				for(String teamId : challengeToApprovedTeamIds.get(challenge)) {
					Team team = synapse.getTeam(teamId);
					log.error("Processing: " + team.getName());
					int added = addUsersToEmailList(team, challenge, true);
					log.error("Emails added: " + added);
				}

				// update All Participants list
				Team team = synapse.getTeam(challengeToAllRegisteredTeamId.get(challenge));
				log.error("Processing: " + team.getName());
				int added = addUsersToEmailList(team, challenge, false);
				log.error("Emails added: " + added);

				// now remove approved users from all participants list to make it an unapproved mailchimp list
				deleteApproved(challenge);
			} catch (Exception e){
				// Something went wrong and we did not process the message.
				log.error("Failed to process evaluation: " + challenge, e);
				throw e;
			}			
		}		
	}

	/*
	 * Private Methods
	 */

	/**
	 * Itempotent
	 * @param team
	 * @param challenge 
	 * @throws NotFoundException 
	 * @throws MailChimpException 
	 * @throws IOException 
	 * @throws SynapseException
	 * @returns the number of emails added 
	 */
	private int addUsersToEmailList(Team team, CurrentChallenges challenge, boolean isApproved) throws NotFoundException, IOException, MailChimpException, SynapseException {
		int added = 0;
		String listId = isApproved ? challengeToMailChimpId.get(challenge) : challengeToUnapprovedMailChimpId.get(challenge);
		if(listId == null) throw getNotFoundException(team);
		
		Set<String> listEmails = getAllListEmails(listId);				
		
		// get all participants in the competition and batch update new ones into the MailChimp list

		long total = Integer.MAX_VALUE; // starting value
		int offset = 0;
		int limit = 100;
		while(offset < total) {
			int toAdd = 0;
			PaginatedResults<TeamMember> batch = synapse.getTeamMembers(team.getId(), null, limit, offset);
			total = batch.getTotalNumberOfResults();
			List<MailChimpObject> mcBatch = new ArrayList<MailChimpObject>();
			//get all user profiles
			List<Long> userIds = new ArrayList<Long>();
			for(TeamMember participant : batch.getResults()) {
				try {
					if(participant.getMember().getIsIndividual()) {
						userIds.add(Long.parseLong(participant.getMember().getOwnerId()));
					}
				} catch (Exception e) {
					log.error("Error retrieving user: "+ participant.getMember().getOwnerId(), e);
				}
			}
			List<UserProfile> userProfiles = synapse.listUserProfiles(userIds);
			for(UserProfile userProfile : userProfiles) {
				try {
					// get user's email and if not in email list already, add
					String participantEmail = userProfile.getEmails().get(0);
					if(participantEmail == null && userProfile.getEmails() != null && userProfile.getEmails().size() > 0)
						participantEmail = userProfile.getEmails().get(0);

					if(isApproved) approvedUserEmails.get(challenge).add(participantEmail); // add approved participants
					if(participantEmail != null && !listEmails.contains(participantEmail)) {
						if(!isApproved && approvedUserEmails.containsKey(participantEmail)) continue;
						MailChimpObject obj = new MailChimpObject();
						obj.put("EMAIL", participantEmail);					
						obj.put("EMAIL_TYPE", "html");
						obj.put("FNAME", userProfile.getFirstName());
						obj.put("LNAME", userProfile.getLastName());
						mcBatch.add(obj);
						toAdd++;
					}
				} catch (Exception e) {
					log.error("Error retrieving user email: "+ userProfile.getOwnerId(), e);
				}
			}

			// add to list AND the overall Dream list
			for(String id : new String[]{listId, OVERALL_DREAM_MAILCHIMP_LIST_ID}) {
				ListBatchSubscribeMethod subscribeRequest = new ListBatchSubscribeMethod();
				subscribeRequest.apikey = mailChimpApiKey;
				subscribeRequest.id = id;
				subscribeRequest.double_optin = false;
				subscribeRequest.update_existing = false;
				subscribeRequest.batch = mcBatch;
				
				mailChimpClient.execute(subscribeRequest);
				if(id != OVERALL_DREAM_MAILCHIMP_LIST_ID) added += toAdd;
			}			
			offset += limit;
		}
		return added;
	}

	private Set<String> getAllListEmails(String listId) throws IOException, MailChimpException {
		Set<String> emails = new HashSet<String>();
				
		// get all subscribed & unsubscribed members of the list
		for(MemberStatus status : new MemberStatus[]{ MemberStatus.subscribed, MemberStatus.unsubscribed}) {
			int offset = 0;
			int limit = 100;
			boolean done = false;	
			while(!done) {
				ListMembersMethod request = new ListMembersMethod();
				request.apikey = mailChimpApiKey;
				request.id = listId;
				request.status = status;
				request.start = offset;
				request.limit = limit;
				
				ListMembersResult result = mailChimpClient.execute(request);
				for(ShortMemberInfo member : result.data) {
					if(member.email != null && !member.email.isEmpty())
						emails.add(member.email);
				}
				
				offset += limit;			
				if(result.total <= offset) done = true;
			}
		}
		return emails;
	}
	
	private NotFoundException getNotFoundException(Team team) {
		return new NotFoundException("Unknown mailing list for team:" + team.getId() + ", " + team.getName());
	}

	private void deleteApproved(CurrentChallenges challenge)
			throws IOException, MailChimpException {
		String unApprovedMailChimpListId = challengeToUnapprovedMailChimpId.get(challenge);					
		ListBatchUnsubscribeMethod deleteBatch = new ListBatchUnsubscribeMethod();
		deleteBatch.apikey = mailChimpApiKey;
		deleteBatch.id = unApprovedMailChimpListId;
		deleteBatch.delete_member = true;
		deleteBatch.emails = new ArrayList<String>(approvedUserEmails.get(challenge));
		deleteBatch.send_goodbye = false;
		deleteBatch.send_notify = false;
		log.error("Unsubscribed already Approved: " + approvedUserEmails.get(challenge).size());
		mailChimpClient.execute(deleteBatch);
	}

	private SynapseClient createSynapseClient() {
		SynapseClient synapseClient = new SynapseClientImpl();
//		synapseClient.setRepositoryEndpoint(StackConfiguration.getRepositoryServiceEndpoint());
//		synapseClient.setAuthEndpoint(StackConfiguration.getAuthenticationServicePublicEndpoint());
		return synapseClient;
	}

}
