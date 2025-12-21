// server rules live here\
/*AllServers:
• IfcommitIndex>lastApplied:incrementlastApplied,apply
log[lastApplied]tostatemachine(§5.3)
• IfRPCrequestorresponsecontainstermT>currentTerm:
setcurrentTerm=T,converttofollower(§5.1)


Followers(§5.2):
• RespondtoRPCsfromcandidatesandleaders
• IfelectiontimeoutelapseswithoutreceivingAppendEntries
RPCfromcurrentleaderorgrantingvotetocandidate:
converttocandidate


Candidates(§5.2):
• Onconversiontocandidate,startelection:
• IncrementcurrentTerm
• Voteforself
• Resetelectiontimer
• SendRequestVoteRPCstoallotherservers
• Ifvotesreceivedfrommajorityofservers:becomeleader
• IfAppendEntriesRPCreceivedfromnewleader:convertto
follower
• Ifelectiontimeoutelapses:startnewelection


Leaders:
• Uponelection:sendinitialemptyAppendEntriesRPCs
(heartbeat)toeachserver;repeatduringidleperiodsto
preventelectiontimeouts(§5.2)
• Ifcommandreceivedfromclient:appendentrytolocallog,
respondafterentryappliedtostatemachine(§5.3)
• Iflastlogindex≥nextIndexforafollower:send
AppendEntriesRPCwithlogentriesstartingatnextIndex
• Ifsuccessful:updatenextIndexandmatchIndexfor
follower(§5.3)
• IfAppendEntriesfailsbecauseofloginconsistency:
decrementnextIndexandretry(§5.3)
• IfthereexistsanNsuchthatN>commitIndex,amajority
ofmatchIndex[i]≥N,andlog[N].term==currentTerm:
setcommitIndex=N(§5.3,§5.4).
