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
import com.ecwid.mailchimp.method.list.ListBatchSubscribeMethod;
import com.ecwid.mailchimp.method.list.ListMembersMethod;
import com.ecwid.mailchimp.method.list.ListMembersResult;
import com.ecwid.mailchimp.method.list.MemberStatus;
import com.ecwid.mailchimp.method.list.ShortMemberInfo;

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
	Map<CurrentChallenges, List<String>> challengeToTeamIds;
	
	public EvaluationMailSyncer(String mailChimpApiKey, String synapseUsername,
			String synapsePassword) throws SynapseException {
		if(mailChimpApiKey == null) throw new IllegalArgumentException("mailChimpApiKey cannot be null");
		if(synapseUsername == null) throw new IllegalArgumentException("synapseUserName cannot be null");
		if(synapsePassword == null) throw new IllegalArgumentException("synapsePassword cannot be null");
		
		this.mailChimpApiKey = mailChimpApiKey;
		this.mailChimpClient = new MailChimpClient();
		this.synapse = new SynapseClientImpl();
		synapse.login(synapseUsername, synapsePassword);
		
		challengeToMailChimpId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToMailChimpId.put(CurrentChallenges.AD1, "7f61028e0e");
		challengeToMailChimpId.put(CurrentChallenges.RA, "3f8f9cadc5");
		challengeToMailChimpId.put(CurrentChallenges.MUTCALL, "aa8f782347");
		challengeToMailChimpId.put(CurrentChallenges.TEST, "8c83f36742");

		challengeToTeamIds = new HashMap<EvaluationMailSyncer.CurrentChallenges, List<String>>();
		challengeToTeamIds.put(CurrentChallenges.AD1, Arrays.asList(new String[]{ "" }));
		challengeToTeamIds.put(CurrentChallenges.RA, Arrays.asList(new String[]{ "" }));
		challengeToTeamIds.put(CurrentChallenges.MUTCALL, Arrays.asList(new String[]{ "" }));
		challengeToTeamIds.put(CurrentChallenges.TEST, Arrays.asList(new String[]{ "" }));
				
	}

	public void sync() {		
		for(CurrentChallenges challenge : CurrentChallenges.values()) {
			try{
				for(String teamId : challengeToTeamIds.get(challenge)) {
					Team team = synapse.getTeam(teamId);
					log.info("Processing: " + team.getName());
					int added = addUsersToEmailList(team, challenge);
					log.info("Emails added: " + added);
				}
			}catch (Throwable e){
				// Something went wrong and we did not process the message.
				log.error("Failed to process evaluation: " + challenge, e);
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
	private int addUsersToEmailList(Team team, CurrentChallenges challenge) throws NotFoundException, IOException, MailChimpException, SynapseException {
		int added = 0;
		String listId = challengeToMailChimpId.get(challenge);
		if(listId == null) throw getNotFoundException(team);
		
		Set<String> listEmails = getAllListEmails(listId);				
		
		// get all participants in the competition and batch update new ones into the MailChimp list

		long total = 1; // starting value
		int offset = 0;
		int limit = 100;
		while(offset < total) {
			int toAdd = 0;
			PaginatedResults<TeamMember> batch = synapse.getTeamMembers(team.getId(), null, offset, limit);
			total = batch.getTotalNumberOfResults();
			List<MailChimpObject> mcBatch = new ArrayList<MailChimpObject>();
			for(TeamMember participant : batch.getResults()) {
				try {
					// get user's email and if not in email list already, add
					if(participant.getMember().getIsIndividual()) {
						UserProfile userProfile = synapse.getUserProfile(participant.getMember().getOwnerId());
						String participantEmail = userProfile.getEmail();
						if(participantEmail != null && !listEmails.contains(participantEmail)) {
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
				subscribeRequest.update_existing = true;
				subscribeRequest.batch = mcBatch;
				
				try {
					mailChimpClient.execute(subscribeRequest);
					if(id != OVERALL_DREAM_MAILCHIMP_LIST_ID) added += toAdd;
				} catch (IOException e) {
					log.error("Error updating MailChimp list for evaluation: " + team.getId(), e);
				} catch (MailChimpException e) {
					log.error("Error updating MailChimp list for evaluation: " + team.getId(), e);
				}
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

}
