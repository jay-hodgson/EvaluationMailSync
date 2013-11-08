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
	static private enum CurrentChallenges { AD1, RA, MUTCALL, TEST };	
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
		this.synapse = new SynapseClientImpl();
		synapse.login(synapseUsername, synapsePassword);
		
		approvedUserEmails = new HashMap<EvaluationMailSyncer.CurrentChallenges, Set<String>>();
		approvedUserEmails.put(CurrentChallenges.AD1, new HashSet<String>());
		approvedUserEmails.put(CurrentChallenges.RA, new HashSet<String>());
		approvedUserEmails.put(CurrentChallenges.MUTCALL, new HashSet<String>());
		approvedUserEmails.put(CurrentChallenges.TEST, new HashSet<String>());
		
		challengeToMailChimpId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToMailChimpId.put(CurrentChallenges.AD1, "7f61028e0e");
		challengeToMailChimpId.put(CurrentChallenges.RA, "3f8f9cadc5");
		challengeToMailChimpId.put(CurrentChallenges.MUTCALL, "aa8f782347");
		challengeToMailChimpId.put(CurrentChallenges.TEST, "8c83f36742");

		challengeToApprovedTeamIds = new HashMap<EvaluationMailSyncer.CurrentChallenges, List<String>>();
		challengeToApprovedTeamIds.put(CurrentChallenges.AD1, Arrays.asList(new String[]{ "2223742" }));
		challengeToApprovedTeamIds.put(CurrentChallenges.RA, Arrays.asList(new String[]{ "2223746" }));
		challengeToApprovedTeamIds.put(CurrentChallenges.MUTCALL, Arrays.asList(new String[]{ "2223745" }));
		challengeToApprovedTeamIds.put(CurrentChallenges.TEST, Arrays.asList(new String[]{ "2223779" }));

		challengeToUnapprovedMailChimpId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToUnapprovedMailChimpId.put(CurrentChallenges.AD1, "644d60525c");
		challengeToUnapprovedMailChimpId.put(CurrentChallenges.RA, "2fd37b3bf3");
		challengeToUnapprovedMailChimpId.put(CurrentChallenges.MUTCALL, "48d7e03a2c");
		challengeToUnapprovedMailChimpId.put(CurrentChallenges.TEST, "46c14b1884");
		
		challengeToAllRegisteredTeamId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToAllRegisteredTeamId.put(CurrentChallenges.AD1, "2223741");
		challengeToAllRegisteredTeamId.put(CurrentChallenges.RA, "2223744");
		challengeToAllRegisteredTeamId.put(CurrentChallenges.MUTCALL, "2223743");
		challengeToAllRegisteredTeamId.put(CurrentChallenges.TEST, "2223780");
		
	}

	public void sync() throws Exception {				
		for(CurrentChallenges challenge : CurrentChallenges.values()) {
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
			for(TeamMember participant : batch.getResults()) {
				try {
					// get user's email and if not in email list already, add
					if(participant.getMember().getIsIndividual()) {
						UserProfile userProfile = synapse.getUserProfile(participant.getMember().getOwnerId());
						String participantEmail = userProfile.getEmail();						
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
					}
				} catch (SynapseException e) {
					log.error("Error retrieving user: "+ participant.getMember().getOwnerId(), e);
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

}
