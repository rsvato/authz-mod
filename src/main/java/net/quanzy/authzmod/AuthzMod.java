package net.quanzy.authzmod;

import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.networking.v1.PacketSender;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.network.ServerPlayNetworkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthzMod implements ModInitializer {
    public static final Logger logger = LoggerFactory.getLogger("authz");

    @Override
    public void onInitialize() {
        logger.info("In onInitialize()");
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {

        });
    }
}
