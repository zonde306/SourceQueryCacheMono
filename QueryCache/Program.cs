using System;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace QueryCache
{
	class ThreadWorkerInfo
	{
		public EndPoint requestingEP = null;
		public byte[] reqPacket = new byte[MainClass.maxPacket];
	};

	class MainClass
	{
		private static byte[] infoCache;
		private static byte[][] playerCache; // Jagged arrays for storing muti-packet responses.
		private static byte[][] rulesCache;
		private static byte[] challengeCode = new byte[4];
		public const int maxPacket = 1400;
		private static int infoQueries;
		private static int otherQueries;
		private static int recvInfoQueries;
		private static int recvOtherQueries;
		private static DateTime lastInfoTime;
		private static DateTime lastPlayersTime;
		private static DateTime lastRulesTime;
		private static DateTime lastPrint;
		private static IPEndPoint serverEP;
		private static Socket serverSock = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
		private static Socket publicSock = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
		private static ReaderWriterLock infoCacheLock = new ReaderWriterLock();
		private static ReaderWriterLock playerCacheLock = new ReaderWriterLock();
		private static ReaderWriterLock rulesCacheLock = new ReaderWriterLock();
		private static Mutex updateInfoCacheLock = new Mutex();
		private static Mutex updatePlayerCacheLock = new Mutex();
		private static Mutex updateRulesCacheLock = new Mutex();

		public static byte[] RequestChallenge()
		{
			if (challengeCode[0] == 0xFF && challengeCode[1] == 0xFF && challengeCode[2] == 0xFF && challengeCode[3] == 0xFF)
			{
				//Check if we've still got the default challenge of 'FF FF FF FF'

				byte[] requestPacket = new byte[9];
				requestPacket[0] = 0xFF;
				requestPacket[1] = 0xFF;
				requestPacket[2] = 0xFF;
				requestPacket[3] = 0xFF;
				requestPacket[4] = 0x55;
				requestPacket[5] = 0xFF;
				requestPacket[6] = 0xFF;
				requestPacket[7] = 0xFF;
				requestPacket[8] = 0xFF;
				byte[] serverResponse = new byte[9];

				try
				{
					serverSock.SendTo(requestPacket, serverEP);
				}
				catch
				{
					return challengeCode;
				}

				try
				{
					serverSock.Receive(serverResponse);
				}
				catch
				{
					return challengeCode;
				}

				if (serverResponse[4] == 0x41)
				{
					// Check the header to see if the server sent a challenge code.
					System.Buffer.BlockCopy(serverResponse, 5, challengeCode, 0, 4); // Copy the challenge code back in to our variable for re-use
					return challengeCode;
				}
			}
			return challengeCode; // Return the cached code we got previously
		}

		public static byte[] BuildRequest(byte queryType)
		{
			byte[] builtQuery = new byte[9];
			builtQuery[0] = 0xFF;
			builtQuery[1] = 0xFF;
			builtQuery[2] = 0xFF;
			builtQuery[3] = 0xFF;
			builtQuery[4] = queryType;
			System.Buffer.BlockCopy(RequestChallenge(), 0, builtQuery, 5, 4);

			return builtQuery;
		}

		public static bool ChallengeIsValid(byte[] requestQuery)
		{
			byte[] challengeCode = RequestChallenge();

			for (int i = 0; i < 4; i++)
			{
				if (!requestQuery[i + 5].Equals(challengeCode[i]))
				{
					return false;
				}
			}
			return true;
		}

		public static bool UpdateCache(byte queryType)
		{
			if (queryType == 0x54)
			{
				// A2S info queries don't need a challenge code
				// We'll build the query packet and send it.
				string queryString = "Source Engine Query";
				byte[] queryStringBytes = new byte[6 + queryString.Length];
				queryStringBytes[0] = 0xFF;
				queryStringBytes[1] = 0xFF;
				queryStringBytes[2] = 0xFF;
				queryStringBytes[3] = 0xFF;
				queryStringBytes[4] = 0x54;
				queryStringBytes[queryStringBytes.Length - 1] = 0x00;
				System.Buffer.BlockCopy(Encoding.Default.GetBytes(queryString), 0,
					queryStringBytes, 5, queryString.Length);

				try
				{
					serverSock.SendTo(queryStringBytes, serverEP);
				}
				catch
				{
					Console.WriteLine("Cannot send info query!");
					return false;
				}
			}
			else
			{
				try
				{
					serverSock.SendTo(BuildRequest(queryType), serverEP); // Every other query type will be sent with a challenge code
				}
				catch
				{
					Console.WriteLine("Cannot send other query!");
					return false;
				}
			}

			byte[] recvBuffer = new byte[maxPacket];
			int packetLen;
			try
			{
				packetLen = serverSock.Receive(recvBuffer);
			}
			catch
			{
				//Console.WriteLine("Cannot Receive SourceEngineQuery!");
				return false;
			}

			// We're not expecting a challenge code to return, but just in case that it does, we'll update our cached code to the new one.
			if (recvBuffer[0] == 0xFF && recvBuffer[1] == 0xFF && recvBuffer[2] == 0xFF && recvBuffer[3] == 0xFF && recvBuffer[4] == 0x41)
			{
				System.Buffer.BlockCopy(recvBuffer, 5, challengeCode, 0, 4);
				return UpdateCache(queryType);
			}

			if (recvBuffer[0] == 0xFE)
			{
				// Check first byte in the packet that indicates a multi-packet response.
				int packetCount = Convert.ToInt32(recvBuffer[8]); // Total number of packets that the server is sending will be at this position.
				switch (recvBuffer[16])
				{
					// The packet type header will be at this position for the first packet.
					case 0x45: // Returned rules list header
					{
						try
						{
							rulesCacheLock.AcquireWriterLock(3000);

							// Initialise our array with same size as the number of packets
							rulesCache = new byte[packetCount][];
							for (int i = 0; i < packetCount; i++)
							{
								rulesCache[i] = new byte[packetLen];
								System.Buffer.BlockCopy(recvBuffer, 0, rulesCache[i], 0, packetLen); // Dump our packet contents in to its place
								if (i < packetCount - 1)
								{
									// Get ready to receive next packet if this isn't the last iteration.
									recvBuffer = new byte[maxPacket];
									try
									{
										packetLen = serverSock.Receive(recvBuffer);
									}
									catch
									{
										//Console.WriteLine("Cannot Receive rules list header!");
										rulesCacheLock.ReleaseWriterLock();
										return false;
									}
								}
							}

							rulesCacheLock.ReleaseWriterLock();
						}
						catch(ApplicationException)
						{ }

						//Console.WriteLine("RuleList cached");
						return true;
					}
					case 0x44: // Returned player list header
					{
						try
						{
							playerCacheLock.AcquireWriterLock(3000);

							playerCache = new byte[packetCount][];
							for (int i = 0; i < packetCount; i++)
							{
								playerCache[i] = new byte[packetLen];
								System.Buffer.BlockCopy(recvBuffer, 0, playerCache[i], 0, packetLen);
								if (i < packetCount - 1)
								{
									recvBuffer = new byte[maxPacket];
									try
									{
										packetLen = serverSock.Receive(recvBuffer);
									}
									catch
									{
										//Console.WriteLine("Cannot Receive player list header!");
										playerCacheLock.ReleaseWriterLock();
										return false;
									}
								}
							}

							//Console.WriteLine("PlayerList cached");
							playerCacheLock.ReleaseWriterLock();
						}
						catch(ApplicationException)
						{ }

						return true;
					}
				}

				// We didn't match anything that we can handle :(
				Console.WriteLine("Receive Unhandled!");
				return false;
			}
			else
			{
				// Handle single packet response.
				switch (recvBuffer[4])
				{
					case 0x49:
					{
						try
						{
							infoCacheLock.AcquireWriterLock(3000);

							try
							{
								infoCache = new byte[packetLen];
								System.Buffer.BlockCopy(recvBuffer, 0, infoCache, 0, packetLen);
							}
							finally
							{
								infoCacheLock.ReleaseWriterLock();
							}
						}
						catch(ApplicationException)
						{ }

						//Console.WriteLine("Info cached");
						return true;
					}
					case 0x44:
					{
						try
						{
							playerCacheLock.AcquireWriterLock(3000);

							try
							{
								playerCache = new byte[1][]; // Initialise our array with a single slot because we only have a single packet to store.
								playerCache[0] = new byte[packetLen];
								System.Buffer.BlockCopy(recvBuffer, 0, playerCache[0], 0, packetLen);
							}
							finally
							{
								playerCacheLock.ReleaseWriterLock();
							}
						}
						catch(ApplicationException)
						{ }

						//Console.WriteLine("Player cached");
						return true;
					}
					case 0x45:
					{
						try
						{
							rulesCacheLock.AcquireWriterLock(3000);

							try
							{
								rulesCache = new byte[1][];
								rulesCache[0] = new byte[packetLen];
								System.Buffer.BlockCopy(recvBuffer, 0, rulesCache[0], 0, packetLen);
							}
							finally
							{
								rulesCacheLock.ReleaseWriterLock();
							}
						}
						catch(ApplicationException)
						{ }

						//Console.WriteLine("Rule cached");
						return true;
					}
				}

				/*
				infoCache = new byte[recvBuffer.Length];
				playerCache = new byte[1][];
				playerCache [0] = new byte[recvBuffer.Length];
				rulesCache = new byte[1][];
				rulesCache [0] = new byte[recvBuffer.Length];
				*/

				Console.WriteLine("Cannot handle single packet response!");
				return false;
			}

			//return false;
		}

		public static EndPoint Init(int proxyPort, IPAddress gameServerIp, int gameServerPort)
		{
			serverEP = new IPEndPoint(gameServerIp, gameServerPort);
			serverSock = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
			publicSock = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

			serverSock.SendTimeout = 100;
			serverSock.ReceiveTimeout = 1000;
			serverSock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, 100);
			serverSock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, 1000);

			publicSock.SendTimeout = 1000;
			publicSock.ReceiveTimeout = 1000;
			publicSock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, 100);
			publicSock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, 1000);

			IPEndPoint localEndPoint = new IPEndPoint(IPAddress.Any, proxyPort);
			IPEndPoint sendingIPEP = new IPEndPoint(IPAddress.Any, 0);

			try
			{
				publicSock.Bind(localEndPoint);
			}
			catch
			{
				//Console.WriteLine("Cannot bind proxy port!");
				return null;
			}
			serverSock.Bind(new IPEndPoint(IPAddress.Any, 0));

			// Give our intial challenge code a value
			challengeCode[0] = 0xFF;
			challengeCode[1] = 0xFF;
			challengeCode[2] = 0xFF;
			challengeCode[3] = 0xFF;

			int maxThread = 0, maxCompletionPortThreads = 0;
			ThreadPool.GetMaxThreads(out maxThread, out maxCompletionPortThreads);

			int minThread = 0, minCompletionPortThreads = 0;
			ThreadPool.GetMinThreads(out minThread, out minCompletionPortThreads);

			Console.WriteLine("Initialization... maxThread {0}/{1}, minThread {2}/{3}",
				maxThread, maxCompletionPortThreads, minThread, minCompletionPortThreads);
			return sendingIPEP;
		}

		public static void HandleQuery(Object data)
		{
			ThreadWorkerInfo info = (ThreadWorkerInfo)data;
			EndPoint requestingEP = info.requestingEP;
			byte[] reqPacket = info.reqPacket;

			switch (reqPacket[4])
			{
				case 0x54: // Info Queries
				{
					Interlocked.Increment(ref infoQueries);

					if (lastInfoTime + TimeSpan.FromSeconds(5) <= DateTime.Now)
					{
						if (updateInfoCacheLock.WaitOne(100))
						{
							// Update our cached values
							if (!UpdateCache(reqPacket[4]))
								return;

							lastInfoTime = DateTime.Now;
							updateInfoCacheLock.ReleaseMutex();
						}
					}

					// If we get this far, we send our cached values.
					try
					{
						infoCacheLock.AcquireReaderLock(100);

						try
						{
							publicSock.SendTo(infoCache, requestingEP);
							Interlocked.Increment(ref recvInfoQueries);
						}
						catch
						{
							return;
						}
						finally
						{
							infoCacheLock.ReleaseReaderLock();
						}
					}
					catch(ApplicationException)
					{ }

					break;
				}
				case 0x55: // Player list
				{
					Interlocked.Increment(ref otherQueries);

					if (!ChallengeIsValid(reqPacket))
					{
						// We check that the client is using the correct challenge code
						try
						{
							publicSock.SendTo(BuildRequest(0x41), requestingEP);
						}
						catch
						{
							return;
						}
						
						// We'll send the client the correct challenge code to use.
						break;
					}

					if (lastPlayersTime + TimeSpan.FromSeconds(3) <= DateTime.Now)
					{
						if (updatePlayerCacheLock.WaitOne(100))
						{
							if (!UpdateCache(reqPacket[4]))
								return;

							lastPlayersTime = DateTime.Now;
							updatePlayerCacheLock.ReleaseMutex();
						}
					}

					try
					{
						playerCacheLock.AcquireReaderLock(100);

						bool success = true;
						for (int i = 0; i < playerCache.Length; i++)
						{
							try
							{
								publicSock.SendTo(playerCache[i], requestingEP);
							}
							catch
							{
								success = false;
								break;
							}
						}

						if(success)
							Interlocked.Increment(ref recvOtherQueries);

						playerCacheLock.ReleaseReaderLock();
					}
					catch(ApplicationException)
					{ }

					break;
				}
				case 0x56: //Rules list
				{
					Interlocked.Increment(ref otherQueries);

					if (!ChallengeIsValid(reqPacket))
					{
						try
						{
							publicSock.SendTo(BuildRequest(0x41), requestingEP);
						}
						catch
						{
							return;
						}
						break;
					}

					if (lastRulesTime + TimeSpan.FromSeconds(10) <= DateTime.Now)
					{
						if (updateRulesCacheLock.WaitOne(100))
						{
							if (!UpdateCache(reqPacket[4]))
								return;

							lastRulesTime = DateTime.Now;
							updateRulesCacheLock.ReleaseMutex();
						}
					}

					try
					{
						rulesCacheLock.AcquireReaderLock(100);

						bool success = true;
						for (int i = 0; i < rulesCache.Length; i++)
						{
							try
							{
								publicSock.SendTo(rulesCache[i], requestingEP);
							}
							catch
							{
								success = false;
								break;
							}
						}

						if(success)
							Interlocked.Increment(ref recvOtherQueries);

						rulesCacheLock.ReleaseReaderLock();
					}
					catch(ApplicationException)
					{ }

					break;
				}
				case 0x57: // Challenge request
				{
					Interlocked.Increment(ref otherQueries);

					try
					{
						// Send challenge response.
						publicSock.SendTo(BuildRequest(0x41), requestingEP);
						Interlocked.Increment(ref recvOtherQueries);
					}
					catch
					{
						return;
					}
					break;
				}
			}

			return;
		}

		public static void Main(string[] args)
		{
			string cmdLine = Environment.GetCommandLineArgs()[0];
			if (args.Length != 3)
			{
				Console.WriteLine("Usage: " + cmdLine + " <proxy port> <gameserver ip> <gameserver port>");
				Environment.Exit(1);
			}
			IPAddress targetIP;
			int targetPort;
			int localPort;
			if (!int.TryParse(args[0], out localPort) || localPort < 1 || localPort > 65535)
			{
				Console.WriteLine("Invalid proxy port!");
				Environment.Exit(2);
			}
			if (!IPAddress.TryParse(args[1], out targetIP))
			{
				Console.WriteLine("Invalid gameserver IP address!");
				Environment.Exit(3);
			}
			if (!int.TryParse(args[2], out targetPort) || targetPort < 1 || targetPort > 65535)
			{
				Console.WriteLine("Invalid gameserver port!");
				Environment.Exit(4);
			}

			EndPoint requestingEP = Init(localPort, targetIP, targetPort);
			if (requestingEP == null)
			{
				Console.WriteLine("Cannot bind proxy port!");
				Environment.Exit(4);
			}

			// Give our time trackers an initial time
			lastInfoTime = DateTime.Now - TimeSpan.FromSeconds(30);
			lastPlayersTime = DateTime.Now - TimeSpan.FromSeconds(30);
			lastRulesTime = DateTime.Now - TimeSpan.FromSeconds(30);
			lastPrint = DateTime.Now;

			//// Main query receiving loop
			while (true)
			{
				byte[] reqPacket = new byte[maxPacket];
				try
				{
					publicSock.ReceiveFrom(reqPacket, ref requestingEP);
				}
				catch
				{
					continue;
				}

				ThreadWorkerInfo info = new ThreadWorkerInfo();
				info.reqPacket = reqPacket;
				info.requestingEP = requestingEP;
				ThreadPool.QueueUserWorkItem(HandleQuery, info);

				if (DateTime.Now.AddSeconds(-10) >= lastPrint)
				{
					int workerThreads = 0, completionPortThreads = 0;
					ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);

					Console.WriteLine("{0}/{1} info queries and {2}/{3} other queries in last {4} seconds, availability {5}/{6}",
						recvInfoQueries, infoQueries, recvOtherQueries, otherQueries,
						(DateTime.Now - lastPrint).Seconds,
						workerThreads, completionPortThreads);

					Interlocked.Exchange(ref infoQueries, 0);
					Interlocked.Exchange(ref otherQueries, 0);
					Interlocked.Exchange(ref recvInfoQueries, 0);
					Interlocked.Exchange(ref recvOtherQueries, 0);
					lastPrint = DateTime.Now;
				}
			}
		}
	}
}
