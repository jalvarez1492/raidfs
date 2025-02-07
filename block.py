import pickle, logging
import fsconfig
import xmlrpc.client, socket, time

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self):

        self.block_servers = {}

        # initialize block cache empty
        self.blockcache = {}

        # EEL 5737
        self.load_analysis = {}

        for i in range(fsconfig.NUM_SERVERS):
            self.load_analysis[i] = 0

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        if fsconfig.PORT:
            PORT = fsconfig.PORT
        else:
            print('Must specify port number')
            quit()

        for i in range(fsconfig.NUM_SERVERS):
            # initialize XMLRPC client connection to raw block server
            server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(PORT + i)
            self.block_servers[i] = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
        
        socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)


    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE

    def SinglePut(self, block_number, block_data, server_id):
        logging.debug("PUT: Server id: " + str(server_id))
        logging.debug("PUT: block number: " + str(block_number))
        self.load_analysis[server_id] += 1

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # ljust does the padding with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # Write block
            # commenting this out as the request now goes to the server
            # self.block[block_number] = putdata
            # call Put() method on the server; code currently quits on any server failure
            try:
                ret = self.block_servers[server_id].Put(block_number, putdata)
            except socket.timeout:
                print("SERVER_TIMED_OUT")
                return -1, "SERVER_TIMEOUT"
            except:
                return -1, "SERVER_DISCONNECTED"
            # update block cache
            # if fsconfig.SHOW_CACHE: print('CACHE_WRITE_THROUGH ' + str(block_number))
            # self.blockcache[block_number] = putdata
            # flag this is the last writer
            # unless this is a release - which doesn't flag last writer
            if block_number != fsconfig.TOTAL_NUM_BLOCKS-1:
                LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 2
                updated_block = bytearray(fsconfig.BLOCK_SIZE)
                updated_block[0] = fsconfig.CID
                try:
                    self.block_servers[server_id].Put(LAST_WRITER_BLOCK % fsconfig.BLOCK_SIZE, updated_block)
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
                    return -1, "SERVER_TIMEOUT"
                except:
                    return -1, "SERVER_DISCONNECTED"
            if ret == -1:
                logging.error('Put: Server returns error')
                quit()
            return 0, None # none error
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def SingleGet(self, block_number, server_id):
        logging.debug("GET: Server id: " + str(server_id))
        logging.debug("GET: block number: " + str(block_number))

        self.load_analysis[server_id] += 1

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
        #     # logging.debug ('\n' + str((self.block[block_number]).hex()))
        #     # commenting this out as the request now goes to the server
        #     # return self.block[block_number]
        #     # call Get() method on the server
        #     # don't look up cache for last two blocks
        #     if (block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (block_number in self.blockcache):
        #         if fsconfig.SHOW_CACHE: print('CACHE_HIT '+ str(block_number))
        #         data = self.blockcache[block_number]
        #     else:
        #         if fsconfig.SHOW_CACHE: print('CACHE_MISS ' + str(block_number))
            try:
                data = self.block_servers[server_id].Get(block_number)
            except socket.timeout:
                print("SERVER_TIMED_OUT")
                return -1, "SERVER_TIMEOUT"
            except:
                return -1, "SERVER_DISCONNECTED"
            # add to cache
            # self.blockcache[block_number] = data
            # return as bytearray
            return bytearray(data), None

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

## RSM: read and set memory equivalent

    def SingleRSM(self, block_number, server_id):
        logging.debug('RSM: ' + str(block_number))

        self.load_analysis[server_id] += 1

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            try:
                data = self.block_servers[server_id].RSM(block_number)
            except socket.timeout:
                print("SERVER_TIMED_OUT")
                return 0, "SERVER_TIMEOUT"
            except:
                return -1, "SERVER_DISCONNECTED"

            return bytearray(data), None

        logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

        ## Acquire and Release using a disk block lock

    def Acquire(self):
        logging.debug('Acquire')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        lockvalue = self.RSM(RSM_BLOCK);
        if lockvalue == -1:
            return -1
        logging.debug("RSM_BLOCK Lock value: " + str(lockvalue))
        while lockvalue[0] == 1:  # test just first byte of block to check if RSM_LOCKED
            logging.debug("Acquire: spinning...")
            lockvalue = self.RSM(RSM_BLOCK);
        # once the lock is acquired, check if need to invalidate cache
        self.CheckAndInvalidateCache()
        return 0

    def Release(self):
        logging.debug('Release')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        # Put()s a zero-filled block to release lock
        self.Put(RSM_BLOCK,bytearray(fsconfig.RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')))
        return 0

    def CheckAndInvalidateCache(self):
        LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 2
        last_writer = self.Get(LAST_WRITER_BLOCK)
        # if ID of last writer is not self, invalidate and update
        if last_writer[0] != fsconfig.CID:
            if fsconfig.SHOW_CACHE: print("CACHE_INVALIDATED")
            self.blockcache = {}
            updated_block = bytearray(fsconfig.BLOCK_SIZE)
            updated_block[0] = fsconfig.CID
            self.Put(LAST_WRITER_BLOCK,updated_block)

    ## HW5 ##

    def VirtualToPhysical(self, virtual_block_number):
        ####### RAID 1 #######
        # server_id = virtual_block_number // (fsconfig.TOTAL_NUM_BLOCKS)
        ####### RAID 4 #######
        server_id = virtual_block_number % (fsconfig.NUM_SERVERS - 1)
        # physical_block_num = virtual_block_number % (fsconfig.TOTAL_NUM_BLOCKS // (fsconfig.NUM_SERVERS - 1)) # WORKS
        ####### RAID 5 #######
        # Code for determining parity server and block number
        level = (virtual_block_number // (fsconfig.NUM_SERVERS - 1)) % fsconfig.NUM_SERVERS
        parity_server_id = fsconfig.NUM_SERVERS - level - 1

        # Block number is same for parity and data in raid 5
        physical_block_num = (virtual_block_number // (fsconfig.NUM_SERVERS - 1))

        # Code for determining data server
        if level == 0:
                raid5_data_server_id  = server_id
        if level == 1:
            if server_id >= 3:
                raid5_data_server_id  = server_id + 1
            else:
                raid5_data_server_id  = server_id
        if level == 2:
            if server_id >= 2:
                raid5_data_server_id  = server_id + 1
            else:
                raid5_data_server_id  = server_id
        if level == 3:
            if server_id >= 1:
                raid5_data_server_id = server_id + 1
            else:
                raid5_data_server_id = server_id
        if level == 4:
                raid5_data_server_id = server_id + 1

        # print("virtual: " + str(virtual_block_number))
        # print("physical: " + str(physical_block_num))
        # print("level: " + str(level))
        # print("data server id: " + str(raid5_data_server_id))
        # print("parity server id: " + str(parity_server_id))

        return (physical_block_num, raid5_data_server_id, parity_server_id)

    def Get(self, virtual_block_number):
        #data, error = self.SingleGet(block_number, server_id)

        ####### RAID 1 #######
        # for raid1_server in range(fsconfig.NUM_SERVERS):
        #     data, error = self.SingleGet(block_number, server_id=raid1_server)

        #     if error == "SERVER_DISCONNECTED":
        #         print("SERVER_DISCONNECTED GET " + str(virtual_block_number))
        #         pass

        #     # Break if first data is not an offline server, meaning server is valid. No need to loop through all
        #     if error != "SERVER_DISCONNECTED":
        #         break

        ##### END RAID 1 #####
        
        ##### RAID 4 #####

        ##### RAID 5 #####        

        if virtual_block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # logging.debug ('\n' + str((self.block[block_number]).hex()))
            # commenting this out as the request now goes to the server
            # return self.block[block_number]
            # call Get() method on the server
            # don't look up cache for last two blocks
            if (virtual_block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (virtual_block_number in self.blockcache):
                if fsconfig.SHOW_CACHE: print('CACHE_HIT '+ str(virtual_block_number))
                data = self.blockcache[virtual_block_number]
            else:
                if fsconfig.SHOW_CACHE: print('CACHE_MISS ' + str(virtual_block_number))
                try:
                    block_number, server_id, _ = self.VirtualToPhysical(virtual_block_number)
                    data, error = self.SingleGet(block_number, server_id)
                    if error == "SERVER_DISCONNECTED":
                        print("SERVER_DISCONNECTED GET " + str(virtual_block_number))
                except socket.timeout:
                    print("SERVER_TIMED_OUT")
                    return -1, "SERVER_TIMEOUT"
                except:
                    return -1, "SERVER_DISCONNECTED"
                # add to cache
                self.blockcache[virtual_block_number] = data
            # return as bytearray
            # return bytearray(data), None
            
                pass
        
        ##### END RAID 4 #####

        if data == -1 and error != "SERVER_DISCONNECTED":
            print("CORRUPTED_BLOCK " + str(virtual_block_number))
            return -1
        else:
            return data
        
    def Put(self, virtual_block_number, block_data):
        # print(virtual_block_number)
        # print(fsconfig.TOTAL_NUM_BLOCKS)
        block_number, raid5_data_server_id, parity_server_id  = self.VirtualToPhysical(virtual_block_number)
        # print("server " + str(server_id))
        #data, error = self.SinglePut(block_number, block_data, server_id=server_id)

        ####### RAID 1 #######
        # for raid1_server in range(fsconfig.NUM_SERVERS):
        #     data, error = self.SinglePut(block_number, block_data, server_id=raid1_server)

        #     if error == "SERVER_TIMEOUT" or error == "SERVER_DISCONNECTED":
        #         print("SERVER_DISCONNECTED PUT " + str(virtual_block_number))
            

        # if data == -1 and error != "SERVER_DISCONNECTED":
        #     print("CORRUPTED_BLOCK " + str(virtual_block_number))
        #     pass
        
        ####### RAID 4 #######

        ####### RAID 5 #######
                
        oldData, error = self.SingleGet(block_number, raid5_data_server_id)
        currParity, error = self.SingleGet(block_number, parity_server_id)
        parity = bytearray(fsconfig.BLOCK_SIZE)
        for i in range(fsconfig.BLOCK_SIZE):
            if(oldData != -1):
                parity[i] = oldData[i] ^ block_data[i]
            else:
                parity[i] = block_data[i] ^ 0
        for i in range(fsconfig.BLOCK_SIZE):
            if(currParity != -1):   
                parity[i] = parity[i] ^ currParity[i]
            else:
                parity[i] = parity[i] ^ 0

        if fsconfig.SHOW_CACHE: print('CACHE_WRITE_THROUGH ' + str(block_number))
        self.blockcache[virtual_block_number] = block_data

        data, error = self.SinglePut(block_number, block_data, raid5_data_server_id)
        
        if error == "SERVER_TIMEOUT" or error == "SERVER_DISCONNECTED":
                print("SERVER_DISCONNECTED PUT " + str(virtual_block_number))

        if data == -1 and error != "SERVER_DISCONNECTED":
            print("CORRUPTED_BLOCK " + str(virtual_block_number))
            pass
        
        data, error = self.SinglePut(block_number, parity, parity_server_id)

        return 0
    
    def RSM(self, virtual_block_number):
        block_number, server_id, _ = self.VirtualToPhysical(virtual_block_number)
        # data = self.SingleRSM(block_number, server_id)
        
        ####### RAID 1 #######
        # for raid1_server in range(fsconfig.NUM_SERVERS):
        ####### RAID 4 #######
        data, error = self.SingleRSM(block_number, server_id)

        #     # if errors are received, pass to next server
        if error == "SERVER_TIMEOUT":
            print("SERVER_TIMEOUT RSM " + str(virtual_block_number))

        if error == "SERVER_DISCONNECTED":
            print("SERVER_DISCONNECTED RSM " + str(virtual_block_number))

        #     if data == -1 and error != "SERVER_DISCONNECTED":
        #         print("CORRUPTED_BLOCK " + str(virtual_block_number))
        #         pass

        #     return data

        if error == "SERVER_TIMEOUT" or error == "SERVER_DISCONNECTED":
            # No need to print again for RAID1, handled in RAID1 loop
            # print("SERVER_DISCONNECTED RSM " + str(virtual_block_number))
            return -1
        
        if data == -1:
            print("CORRUPTED_BLOCK " + str(virtual_block_number))
            return -1

        return data

    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
