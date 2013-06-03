package org.synapse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.sagebionetworks.client.Synapse;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.evaluation.model.Evaluation;
import org.sagebionetworks.evaluation.model.Participant;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.SchemaCache;
import org.sagebionetworks.repo.model.UserGroup;
import org.sagebionetworks.repo.model.UserProfile;
import org.sagebionetworks.repo.web.NotFoundException;
import org.sagebionetworks.schema.ObjectSchema;

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
	static private enum CurrentChallenges { HPN, TOXICOGENETICS, WHOLECELL, TEST };
	
	String mailChimpApiKey;
	MailChimpClient mailChimpClient;
	Synapse synapse;
	Map<CurrentChallenges, String> challengeToMailChimpId;
	Map<CurrentChallenges, String> challengeToEvaluationId;
	
	public EvaluationMailSyncer(String mailChimpApiKey, String synapseUsername,
			String synapsePassword) throws SynapseException {
		if(mailChimpApiKey == null) throw new IllegalArgumentException("mailChimpApiKey cannot be null");
		if(synapseUsername == null) throw new IllegalArgumentException("synapseUserName cannot be null");
		if(synapsePassword == null) throw new IllegalArgumentException("synapsePassword cannot be null");
		
		this.mailChimpApiKey = mailChimpApiKey;
		this.mailChimpClient = new MailChimpClient();
		this.synapse = new Synapse();
		synapse.login(synapseUsername, synapsePassword);
		
		challengeToMailChimpId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToMailChimpId.put(CurrentChallenges.HPN, "78979af628");
		challengeToMailChimpId.put(CurrentChallenges.TOXICOGENETICS, "ca2a921c6f");
		challengeToMailChimpId.put(CurrentChallenges.WHOLECELL, "5a2d90e13e");
		challengeToMailChimpId.put(CurrentChallenges.TEST, "8c83f36742");
		
		challengeToEvaluationId = new HashMap<EvaluationMailSyncer.CurrentChallenges, String>();
		challengeToEvaluationId.put(CurrentChallenges.HPN, "1867644");
		challengeToEvaluationId.put(CurrentChallenges.TOXICOGENETICS, "1867645");
		challengeToEvaluationId.put(CurrentChallenges.WHOLECELL, "1867647");
		challengeToEvaluationId.put(CurrentChallenges.TEST, "1901529");
		
	}

	public void sync() {		
		for(CurrentChallenges challenge : CurrentChallenges.values()) {
			try{		
				Evaluation eval = synapse.getEvaluation(challengeToEvaluationId.get(challenge));
				log.info("Processing: " + eval.getName());
				int added = addUsersToEmailList(eval, challenge);
				log.info("Emails added: " + added);
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
	 * @param evaluation
	 * @param challenge 
	 * @throws NotFoundException 
	 * @throws MailChimpException 
	 * @throws IOException 
	 * @throws SynapseException
	 * @returns the number of emails added 
	 */
	private int addUsersToEmailList(Evaluation evaluation, CurrentChallenges challenge) throws NotFoundException, IOException, MailChimpException, SynapseException {
		int added = 0;
		String listId = challengeToMailChimpId.get(challenge);
		if(listId == null) throw getNotFoundException(evaluation);
		
		Set<String> listEmails = getAllListEmails(listId);				
		
		// get all participants in the competition and batch update new ones into the MailChimp list
		long total = synapse.getParticipantCount(evaluation.getId());
		int offset = 0;
		int limit = 100;
		while(offset < total) {
			int toAdd = 0;
			PaginatedResults<Participant> batch = synapse.getAllParticipants(evaluation.getId(), offset, limit);			
			ListBatchSubscribeMethod subscribeRequest = new ListBatchSubscribeMethod();
			List<MailChimpObject> mcBatch = new ArrayList<MailChimpObject>();
			for(Participant participant : batch.getResults()) {
				try {
					// get user's email and if not in email list already, add
					UserProfile userProfile = synapse.getUserProfile(participant.getUserId());
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
				} catch (SynapseException e) {
					log.error("Error retrieving user: "+ participant.getUserId(), e);
				}
			}
			subscribeRequest.apikey = mailChimpApiKey;
			subscribeRequest.id = listId;
			subscribeRequest.double_optin = false;
			subscribeRequest.update_existing = true;
			subscribeRequest.batch = mcBatch;
			
			try {
				mailChimpClient.execute(subscribeRequest);
				added += toAdd;
			} catch (IOException e) {
				log.error("Error updating MailChimp list for evaluation: " + evaluation.getId(), e);
			} catch (MailChimpException e) {
				log.error("Error updating MailChimp list for evaluation: " + evaluation.getId(), e);
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
	
	private NotFoundException getNotFoundException(Evaluation evaluation) {
		return new NotFoundException("Unknown mailing list for evaluation:" + evaluation.getId() + ", " + evaluation.getName());
	}

}
