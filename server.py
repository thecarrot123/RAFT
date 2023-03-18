import raft_pb2_grpc as pb2_grpc
from concurrent import futures
import raft_pb2 as pb2
import threading
import random
import time
import grpc
import sys

TIMEOUT_SEC = 0.1
STUBS_LIST = []
CHANNEL_LIST = []
ID_LIST = []
IP_LIST = []
PORT_LIST = []

def get_parameters():
	try:
		id = sys.argv[1]
		configLines = open("config.conf","r").readlines()
		global SERVER_NUMBER
		SERVER_NUMBER = len(configLines)
		for line in configLines:
			words = line.split()
			if words[0] == id:
				ip = words[1]
				port = words[2]
				return int(id), ip, int(port)
		raise Exception("error")
	except Exception:
		print("Usage: python3 server.py <id>")
		exit(0)

def create_stub(ip, port):
	try:
		channel = grpc.insecure_channel('{}:{}'.format(ip, port))
		grpc.channel_ready_future(channel).result(timeout=TIMEOUT_SEC)
		stub = pb2_grpc.RaftStub(channel)
		return stub, channel
	except Exception as e:
		return None, None

def readFile(id):
	IP_LIST.clear()
	PORT_LIST.clear()
	ID_LIST.clear()
	file = open("config.conf","r")
	configLines = file.readlines()
	file.close()
	for line in configLines:
		words = line.split()
		if int(words[0]) == id:
			continue
		ID_LIST.append(int(words[0]))
		IP_LIST.append(words[1])
		PORT_LIST.append(int(words[2]))

def create_stubs(id):
	STUBS_LIST.clear()
	CHANNEL_LIST.clear()
	for i in range(SERVER_NUMBER-1):
		if ID_LIST[i] == id:
			continue
		stub, channel = create_stub(IP_LIST[i], PORT_LIST[i])
		STUBS_LIST.append(stub)
		CHANNEL_LIST.append(channel)

def refresh_stub(index):
	global STUBS_LIST
	global CHANNEL_LIST
	stub, channel = create_stub(IP_LIST[index], PORT_LIST[index])
	STUBS_LIST[index] = stub
	CHANNEL_LIST[index] = channel
	return stub


def collect_votes(term,candidateId):
	cnt = 0
	down = 0
	for i in range(SERVER_NUMBER-1):
		try:
			stub = refresh_stub(i)
			if stub == None:
				down += 1
				continue
			resp = stub.RequestVote(pb2.RequestVoteReq(
				term = term,
				candidateId = candidateId,
				lastLogIndex = raft_servicer.log[-1][0],
				lastLogTerm = raft_servicer.log[-1][1]))
			if resp.result:
				cnt += 1
		except Exception as e:
			down += 1
	return cnt, down

def entryObjectsList(entries):
	objects = []
	for entry in entries:
		objects.append(pb2.Entry(index=entry[0], term=entry[1], command=entry[2]))
	return objects

def send_entries():
	for i in range(SERVER_NUMBER-1):
		try:
			stub = refresh_stub(i)
			if stub == None:
				continue
			matchIndex = raft_servicer.matchIndex[i]
			nextIndex = raft_servicer.nextIndex[i]
			resp = None
			if matchIndex == len(raft_servicer.log)-1:
				reqObject = pb2.AppendEntriesReq(
					term = raft_servicer.term, 
					leaderId = raft_servicer.id, 
					prevLogIndex = raft_servicer.log[-1][0],
					prevLogTerm = raft_servicer.log[-1][1],
					leaderCommit = raft_servicer.commitIndex,)
				resp = stub.AppendEntries(reqObject)
				if resp != None and resp.success:
					raft_servicer.matchIndex[i] = len(raft_servicer.log)-1
					raft_servicer.nextIndex[i] = raft_servicer.matchIndex[i]+1
				elif resp != None and resp.term > raft_servicer.term:
					raft_servicer.term = resp.term
					raft_servicer.make_follower(raft_servicer.id)
				elif resp != None:
					raft_servicer.matchIndex[i] = 0
					raft_servicer.nextIndex[i] = 1
				else:
					print("Error")
			elif matchIndex < len(raft_servicer.log)-1:
				reqObject = pb2.AppendEntriesReq(
					term = raft_servicer.term, 
					leaderId = raft_servicer.id, 
					prevLogIndex = raft_servicer.log[matchIndex][0],
					prevLogTerm = raft_servicer.log[matchIndex][1],
					leaderCommit = raft_servicer.commitIndex,
					entries=entryObjectsList(raft_servicer.log[matchIndex+1:]))
				resp = stub.AppendEntries(reqObject)
				if resp != None and resp.success:
					raft_servicer.matchIndex[i] = len(raft_servicer.log)-1
					raft_servicer.nextIndex[i] = raft_servicer.matchIndex[i]+1
				elif resp != None and resp.term > raft_servicer.term:
					raft_servicer.term = resp.term
					raft_servicer.make_follower(raft_servicer.id)
				elif resp != None:
					raft_servicer.matchIndex[i] = 0
					raft_servicer.nextIndex[i] = 1
				else:
					print("Error")
		except Exception as e:
			pass
	

class RaftServicer(pb2_grpc.RaftServicer):
	def __init__(self, id, ip, port):
		self.id = id
		self.ip = ip
		self.port = port
		self.term = 0
		self.timer = random.randint(150,300)
		self.state = "follower"
		self.leader = id
		self.voted_for = None
		self.commitIndex = 0
		self.lastApplied = 0
		self.log = [(0,0,"create log")]
		self.nextIndex = [self.log[-1][0] for _ in range(SERVER_NUMBER-1)]
		self.matchIndex = [0 for _ in range(SERVER_NUMBER-1)]
		self.data = {}
		set_time()
		create_stubs(id)
	
	def make_candidate(self):
		self.term += 1
		self.state = "candidate"
		print(f"I am a candidate. Term: {self.term}")
		self.voted_for = self.id
		print(f"Voted for node {self.voted_for}")
		votes, _ = collect_votes(self.term,self.id)
		votes += 1
		if votes > SERVER_NUMBER/2:
			print(f"Votes received")
			self.make_leader()
		else:
			self.voted_for = None
			self.timer = random.randint(150,300)
			self.make_follower(self.id)

	def make_follower(self, leaderId):
		self.leader = leaderId
		self.state = "follower"
		print(f"I am a follower. Term: {self.term}")
		set_time()

	def make_leader(self):
		self.state = "leader"
		self.leader = self.id
		print(f"I am a leader. Term: {self.term}")
		set_time()

	def timeout(self):
		if self.state == "follower":
			print("the leader is dead")
			set_time()
			self.make_candidate()

	def RequestVote(self, request, context):
		if (request.term > self.term 
		 or (request.term == self.term and self.voted_for == None)):
			self.term = request.term
			if request.lastLogIndex < self.log[-1][0]:
				return pb2.RequestVoteResp(term = self.term, result = False)
			if request.lastLogTerm != self.log[-1][1]:
				return pb2.RequestVoteResp(term = self.term, result = False)
			self.voted_for = request.candidateId 
			print(f"Voted for node {self.voted_for}")
			self.make_follower(request.candidateId)
			return pb2.RequestVoteResp(term = self.term, result = True)

		return pb2.RequestVoteResp(term = self.term, result = False)
	
	def AppendEntries(self, request, context):
		if request.term > self.term:
			set_time()
			self.term = request.term
			self.leader = request.leaderId
			if request.prevLogIndex > len(self.log):
				return pb2.AppendEntriesResp(term = self.term, success = False)
			if self.receive_entries(request.entries, request.leaderCommit):
				return pb2.AppendEntriesResp(term = self.term, success = True)
			else:
				return pb2.AppendEntriesResp(term = self.term, success = False)

		if request.term == self.term:
			set_time()
			self.leader = request.leaderId
			if request.prevLogIndex > len(self.log):
				return pb2.AppendEntriesResp(term = self.term, success = False)
			if self.receive_entries(request.entries, request.leaderCommit):
				return pb2.AppendEntriesResp(term = self.term, success = True)
			else:
				return pb2.AppendEntriesResp(term = self.term, success = False)
		return pb2.AppendEntriesResp(term = self.term, success = False)
	
	def receive_entries(self, entries, leaderCommit):
		try:
			for entry in entries:
				index = entry.index
				term = entry.term
				command = entry.command
				if index > len(self.log):
					return False
				if index == len(self.log):
					self.log.append((index,term,command))
					continue
				if index != self.log[index][0]:
					return False
				if term != self.log[index][1] or command != self.log[index][2]:
					self.log[index] = (index,term,command)

			self.commitIndex = min(leaderCommit,self.log[-1][0])
			self.apply_commands()
			return True
		except Exception as e:
			return False

	def apply_commands(self):
		while self.commitIndex > self.lastApplied:
			self.lastApplied += 1
			command = self.log[self.lastApplied][2].split()
			self.data[command[1]] = int(command[2])


	def GetLeader(self, request, context):
		print(f"Command from client: getleader")
		configLines = open("config.conf","r").readlines()
		for line in configLines:
			words = line.split()
			if self.leader == int(words[0]):
				return pb2.GetLeaderResp(leaderId=self.leader, address=f"{words[1]}:{words[2]}")
	
	def Suspend(self, request, context):
		print(f"Command from client: suspend {request.period}")
		stop_threads()
		time.sleep(request.period)
		start_threads()
		return pb2.SuspendResp()
	
	def get_leaders_stub(self):
		for itr in range(SERVER_NUMBER-1):
			if ID_LIST[itr] == self.leader:
				return refresh_stub(itr)
		return None

	def SetVal(self, request, context):
		if self.state != "leader":
			for cnt in range(10):
				try:
					stub = self.get_leaders_stub()
					return stub.SetVal(pb2.SetValReq(key=request.key, value=request.value))
				except Exception as e:
					pass
			return pb2.SetValResp(success=False)
		index = self.log[-1][0] + 1
		term = self.term
		command = f"SetVal {request.key} {request.value}"
		self.log.append((index, term, command))
		return pb2.SetValResp(success=True)
	
	def GetVal(self, request, context):
		if self.state != "leader":
			for cnt in range(10):
				try:
					stub = self.get_leaders_stub()
					return stub.GetVal(pb2.GetValReq(key=request.key))
				except Exception as e:
					pass
			return pb2.GetValResp(success=False, value=0)
		try:
			return pb2.GetValResp(success=True, value=self.data[request.key])
		except Exception:
			return pb2.GetValResp(success=False, value=0)


def current_time():
	return time.time()*1000

def set_time():
	global LAST_TIMER
	LAST_TIMER = current_time()

def check_timer():
	if LAST_TIMER + raft_servicer.timer >= current_time():
		return True
	return False

def handle_time_checker():
	while THREAD_RUNNING:
		if not check_timer():
			raft_servicer.timeout()
			set_time()
		time.sleep(0.05)

def start_timer_thread():
	global THREAD
	THREAD = threading.Thread(target=handle_time_checker,args=())
	set_time()
	THREAD.start()

def handle_leader():
	while THREAD_RUNNING:
		time.sleep(0.05)
		if raft_servicer.state == "leader":
			send_entries()

def start_leader_thread():
	global LEADER_THREAD
	LEADER_THREAD = threading.Thread(target=handle_leader,args=())
	LEADER_THREAD.start()

def handle_commit_check():
	while THREAD_RUNNING:
		try:
			if raft_servicer.state == "leader":
				cnt = 1
				mn = 10000000000000
				for i in range(SERVER_NUMBER-1):
					if raft_servicer.matchIndex[i] > raft_servicer.commitIndex:
						mn = min(raft_servicer.matchIndex[i],mn)
						cnt += 1
				if cnt > SERVER_NUMBER/2:
					raft_servicer.commitIndex = mn
					raft_servicer.apply_commands()
		except Exception as e:
			pass
		time.sleep(1)

def start_commit_thread():
	global COMMIT_THREAD
	COMMIT_THREAD = threading.Thread(target=handle_commit_check,args=())
	COMMIT_THREAD.start()
	
def start_threads():
	time.sleep(1)
	global THREAD_RUNNING
	THREAD_RUNNING = True
	start_timer_thread()
	start_leader_thread()
	start_commit_thread()

def stop_threads():
	global THREAD_RUNNING
	THREAD_RUNNING = False
	THREAD.join()
	LEADER_THREAD.join()
	COMMIT_THREAD.join()

def terminate():
	stop_threads()
	print("Terminating")
	exit(0)

if __name__ == "__main__":
	id ,ip, port = get_parameters()
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
	readFile(id)
	raft_servicer = RaftServicer(id,ip,port)
	pb2_grpc.add_RaftServicer_to_server(raft_servicer, server)
	try:
		server.add_insecure_port(f'{ip}:{port}')
		server.start()
		print(f"Server is started at {ip}:{port}")
	except Exception:
		print("Error: failed to start the server")
		exit(0)
	while True:
		try:
			start_threads()
			server.wait_for_termination()
		except KeyboardInterrupt:
			terminate()