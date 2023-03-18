import grpc
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2

TIMEOUT_SEC = 1

def create_stub(ip,port):
	try:
		channel = grpc.insecure_channel('{}:{}'.format(ip, port))
		grpc.channel_ready_future(channel).result(timeout=TIMEOUT_SEC)
		stub = pb2_grpc.RaftStub(channel)
		#stub.get_chord_info(pb2.GetChordInfoRequest())
		return stub, channel
	except Exception as e:
		return None, None

def connect(cmd):
	if len(cmd) != 3:	
		print("Usage: connect <ip address> <port>")
		return None, None
	stub, channel = create_stub(cmd[1],cmd[2])
	print(cmd[1],cmd[2])
	if(stub != None):
		return stub, channel
	print(f"The server {cmd[1]} is unavailable")
	return None, None

def check_command(input_words, cmd, length):
	try:
		if input_words[0] != cmd or len(input_words) != length:
			return False
		return True
	except Exception:
		return False

def check_suspend_command(input_words):
	if check_command(input_words,"suspend",2) and input_words[1].isdigit():
		return True
	return False

def check_getval_command(input_words):
	if check_command(input_words,"getval",2):
		return True
	return False

def check_setval_command(input_words):
	if check_command(input_words,"setval",3) and input_words[2].isdigit():
		return True
	return False

def getLeader(stub):
	return stub.GetLeader(pb2.GetLeaderReq())

def suspend(period,stub):
	stub.Suspend(pb2.SuspendReq(period=period))

HELP = """
Available commands:
connect <ip> <port>
setval <key> <val>
getval <key>
getleader
"""

if __name__ == '__main__':
	stub_type = "Error"
	channel = None
	stub = None
	while(True):
		try:
			input_line = input("> ")
			input_words = input_line.split()
			if check_command(input_words,"connect",3):
				stub, channel = connect(input_words)
			elif check_command(input_words,"getleader",1):
				leader = getLeader(stub) # new line
				print(leader.leaderId, leader.address) # new line
			elif check_suspend_command(input_words):
				suspend(int(input_words[1]),stub)
			elif check_command(input_words,"quit",1):
				if channel != None:
					channel.close()
				exit(0)
			elif check_setval_command(input_words):
				print(stub.SetVal(pb2.SetValReq(key=input_words[1],value=int(input_words[2]))).value)
			elif check_getval_command(input_words):
				print(stub.GetVal(pb2.GetValReq(key=input_words[1])).value)
			else:
				print("Unknown command")
				print(HELP)
		except KeyboardInterrupt:
			print("Terminating")
			if channel != None:
					channel.close()
			exit(0)
		except Exception:
			print("Server not connected or not responding")
