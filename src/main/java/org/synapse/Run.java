package org.synapse;


/**
 * Hello world!
 *
 */
public class Run 
{
    public static void main( String[] args ) throws Exception {
    	if(args.length != 3) throw new IllegalArgumentException(usage());
    	String mailChimpApiKey = args[0];
    	String synapseUsername = args[1];
    	String synapsePassword = args[2];
    	if(mailChimpApiKey == null || synapseUsername == null || synapsePassword == null) throw new IllegalArgumentException(usage()); 
        EvaluationMailSyncer syncer = new EvaluationMailSyncer(mailChimpApiKey, synapseUsername, synapsePassword);
        syncer.sync();
    }

	private static String usage() {
		return "Usage: java -jar EvaluationMailSyncer.jar <MailChimp API key> <Synapse username> <Synapse Password>";
	}
}
